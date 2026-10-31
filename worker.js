// Polymarket -> Telegram (e-sports)
// Module Worker (Cloudflare Workers) - v2.2: v2.1 (tópico por evento) + /status + lista com msgIds + DRY_RUN seguro
// Cron recomendado: * * * * *
//
// Secrets (Worker Settings > Variables and Secrets):
//   CHAVE                  token privado para proteger POST /config e POST /notify
//   TELEGRAM_BOT_TOKEN     token do bot; o bot deve ser administrador com gestão de tópicos
//   TELEGRAM_CHAT_ID       ID do supergrupo com tópicos ativados
//   TELEGRAM_TOPICO_RESUMO (opcional) ID do tópico do resumo diário
//   DRY_RUN                (opcional) só vale se for exatamente "1": registra no log e NÃO fala com o Telegram
//
// Binding KV (namespace binding name: CFG). Para testar em paralelo, use um namespace SEPARADO:
//   CFG / CFG                  configuração das carteiras
//   CFG / estado4              fila, cursores, deduplicação, tópicos, resumo do dia, eventos
//   CFG / lista_carteiras      mensagens "CARTEIRAS MONITORADAS" ({ msgIds: [...], hash, dia })
//   CFG / topico_atualizacoes  ID do tópico "Atualizações"
//
// Gravações (plano grátis: 1.000 por dia, renovam à meia-noite UTC):
//   - estado4 só é gravado quando algo muda de verdade, ou no checkpoint (15 min);
//   - rodada com aviso pronto grava ANTES de enviar: se essa gravação falhar, nada é enviado;
//   - depois de enviar, grava de novo para tirar da fila o que saiu.
// Sem trava no KV: cada rodada tem prazo (PRAZO_*) para terminar antes da próxima.
//
// Novidades v2.2:
//   - GET /status: healthcheck público (200 se o checkpoint tem menos de 20 min; 503 caso contrário);
//   - lista de carteiras: guarda todos os msgIds, edita cada bloco no lugar, apaga os que sobram
//     (sem reenviar páginas e sem acumular mensagens duplicadas no tópico);
//   - DRY_RUN: IDs falsos ficam numa faixa reconhecível (>= 9e12) e são descartados sozinhos
//     quando o DRY_RUN é desligado; o ID falso do tópico "Atualizações" nunca é gravado;
//   - coleta: a janela inicial de 1 h só é perdida depois que a carteira concluiu uma coleta;
//   - tópico: ao recriar um tópico apagado, o placar acumulado é preservado.

const DATA_API = "https://data-api.polymarket.com";
const GAMMA_API = "https://gamma-api.polymarket.com";
const FUSO = "America/Sao_Paulo";

const KV_CFG = "CFG";
const KV_ESTADO = "estado4";
const KV_ESTADO_ANTIGO = "estado3";
const KV_TOPICOS_LEGADO = "topicos";
const KV_LISTA = "lista_carteiras";

const VERSAO = "2026-10-31-v2.2-mesclada";
const NOME_TOPICO_ATUALIZACOES = "Atualizações";

const ESPORT = /(?:^|[-_\s/])(?:league[-_\s]?of[-_\s]?legends|lol|counter[-_\s]?strike|cs2|csgo|cs-go|dota[-_\s]?2?|valorant|rainbow[-_\s]?six|r6siege|r6|call[-_\s]?of[-_\s]?duty|cod(?:mw\d?)?|overwatch|ow2|rocket[-_\s]?league|rlcs|starcraft|mobile[-_\s]?legends|mlbb|honor[-_\s]?kings|hok|king[-_\s]?glory|kog|wild[-_\s]?rift|arena[-_\s]?of[-_\s]?valor)(?:$|[-_\s/])/i;

const CARTEIRAS_POR_CICLO = 20;
const LIMITE_PAGINA = 500;
const CONCORRENCIA = 4;
const SOBREPOSICAO_S = 300;
const JANELA_INICIAL_S = 3600;     // carteira nova: quanto do passado enviar
const JANELA_MAXIMA_S = 3 * 3600;  // depois de uma falha longa, não recupera mais que isso
const MAX_PENDENTES = 3000;
const MAX_AVISOS = 6;
const MAX_TENTATIVAS = 8;          // falha passageira: depois disso, o aviso é descartado (fica no log)
const DIAS_TOPICO = 30;
const TIMEOUT_FETCH_MS = 8000;
const TENTATIVAS_METADADOS = 3;
const RETRY_METADADOS_S = 30;
const EV_VALIDADE_MS = 60 * 60 * 1000; // dados do evento (Gamma): renovam a cada 1 h
const EV_FALHA_MS = 5 * 60 * 1000;     // evento não encontrado: tenta de novo em 5 min
const CHECKPOINT_MS = 15 * 60 * 1000;
const STATUS_VELHO_MS = 20 * 60 * 1000; // /status devolve 503 acima disso
const PRAZO_COLETA_MS = 20000;         // depois disso, não começa novas carteiras na rodada
const PRAZO_ENVIO_MS = 35000;          // depois disso, não começa novos avisos na rodada
const ESPERA_MAX_429_S = 15;           // espera do Telegram acima disso vai para a fila, não para a rodada
const FALHAS_COLETA_RESET = 5;         // falhas seguidas na Data API: descarta cursor e recomeça
const MAX_LISTA_CHARS = 3800;          // margem sobre o limite de 4.096 do Telegram
const ID_FALSO_MIN = 9e12;             // IDs gerados pelo DRY_RUN ficam acima disso (os reais são bem menores)

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Authorization, Content-Type",
  "Access-Control-Max-Age": "86400",
};

const jsonResp = (dados, status = 200) => new Response(JSON.stringify(dados), {
  status,
  headers: { ...CORS, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" },
});
const textResp = (texto, status = 200) => new Response(texto, {
  status,
  headers: { ...CORS, "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" },
});

// DRY_RUN só liga com exatamente "1" (evita que "0" ou "false" ativem por engano).
const dryRun = env => String((env && env.DRY_RUN) ?? "").trim() === "1";
const idFalso = v => Number(v) >= ID_FALSO_MIN;

// Contadores da rodada atual (vão para o log e para a resposta de /notify).
let CONTADOR = null;
const contar = (chave, n = 1) => { if (CONTADOR) CONTADOR[chave] = (CONTADOR[chave] || 0) + n; };
const kvGet = (env, chave) => { contar("kvLeitura"); return env.CFG.get(chave); };
const kvPut = (env, chave, valor) => { contar("kvGravacao"); return env.CFG.put(chave, valor); };

const esc = v => String(v ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const truncar = (v, n) => { const s = String(v ?? ""); return s.length > n ? s.slice(0, n - 1) + "…" : s; };
const pausa = ms => new Promise(resolve => setTimeout(resolve, ms));
const agoraS = () => Math.floor(Date.now() / 1000);
const chaveTexto = v => String(v ?? "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
const limpar = (env, e) => {
  let s = String(e && e.message ? e.message : e);
  for (const v of [env && env.TELEGRAM_BOT_TOKEN, env && env.CHAVE]) if (v) s = s.split(v).join("***");
  return s;
};
const numeroBR = (v, casas = 2) => Number(v || 0).toLocaleString("pt-BR", {
  minimumFractionDigits: casas, maximumFractionDigits: casas,
});
const moeda = v => "US$ " + numeroBR(v, 2);
const pct = p => numeroBR(Number(p || 0) * 100, 1) + "%";
const quantidade = v => numeroBR(v, 3);
const abrevW = w => {
  const s = String(w || "");
  return s.length >= 12 ? s.slice(0, 6) + "…" + s.slice(-4) : s;
};

const fmtHora = new Intl.DateTimeFormat("pt-BR", { timeZone: FUSO, hour: "2-digit", minute: "2-digit", hourCycle: "h23" });
const fmtHoraSeg = new Intl.DateTimeFormat("pt-BR", { timeZone: FUSO, hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" });
const fmtDiaPartes = new Intl.DateTimeFormat("en-CA", { timeZone: FUSO, year: "numeric", month: "2-digit", day: "2-digit" });
const horaBR = ms => fmtHora.format(new Date(ms));
const horaSegBR = ms => fmtHoraSeg.format(new Date(ms));

function diaBR(ms) {
  const d = new Date(Number(ms));
  if (!Number.isFinite(d.getTime())) return "0000-00-00";
  const p = Object.fromEntries(fmtDiaPartes.formatToParts(d).map(x => [x.type, x.value]));
  if (!p.year || !p.month || !p.day) return "0000-00-00";
  return `${p.year}-${p.month}-${p.day}`;
}
const diaCurtoBR = ms => {
  const iso = diaBR(ms);
  return iso === "0000-00-00" ? "??/??" : `${iso.slice(8, 10)}/${iso.slice(5, 7)}`;
};
const diaComAnoBR = iso => {
  const m = String(iso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? `${m[3]}/${m[2]}/${m[1]}` : "data não identificada";
};

function paraMs(v) {
  if (v === null || v === undefined || v === "") return 0;
  if (typeof v === "number") return Number.isFinite(v) ? (v > 1e12 ? v : v * 1000) : 0;
  let s = String(v).trim();
  if (/^\d+$/.test(s)) { const n = Number(s); return n > 1e12 ? n : n * 1000; }
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) s += "T00:00:00Z";
  else {
    s = s.replace(" ", "T").replace(/([+-]\d{2})$/, "$1:00");
    if (!/(Z|[+-]\d{2}:?\d{2})$/i.test(s)) s += "Z";
  }
  const t = Date.parse(s);
  return Number.isNaN(t) ? 0 : t;
}
function dataValor(v) {
  const s = String(v ?? "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const ms = paraMs(v);
  return ms ? diaBR(ms) : null;
}
function paraMsHorario(v) {
  if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}$/.test(v.trim())) return 0;
  return paraMs(v);
}

function normalizar(t) {
  if (!t || typeof t !== "object") return null;
  const size = Number(t.size);
  const price = Number(t.price);
  return {
    w: String(t.proxy_wallet || t.proxyWallet || t.user || "").toLowerCase(),
    cond: String(t.condition_id || t.conditionId || t.condition || "").toLowerCase(),
    asset: String(t.asset || t.token_id || t.tokenId || ""),
    eventSlug: String(t.event_slug || t.eventSlug || "").trim().toLowerCase(),
    slug: String(t.slug || t.market_slug || t.marketSlug || "").trim().toLowerCase(),
    titulo: String(t.title || t.market || t.question || ""),
    outcome: String(t.outcome || t.outcome_name || "?"),
    idx: Number(t.outcome_index ?? t.outcomeIndex ?? 999),
    side: String(t.side || "").toUpperCase(),
    size: Number.isFinite(size) ? size : 0,
    price: Number.isFinite(price) ? price : 0,
    ts: Math.floor(paraMs(t.timestamp) / 1000),
    tx: String(t.transaction_hash || t.transactionHash || t.txHash || t.tx_hash || ""),
  };
}
// Chave de deduplicação: 9 campos separados por "|". O campo de índice 5 é o timestamp.
function chaveTrade(n) {
  return [
    n.tx || "sem-tx", n.asset || "sem-asset", n.cond, n.idx, n.side, n.ts,
    n.size.toFixed(6), n.price.toFixed(6), chaveTexto(n.outcome),
  ].join("|");
}
function tsDaChave(k) {
  const p = String(k).split("|");
  return p.length === 9 ? Number(p[5]) : NaN;
}

function eEsports(n) {
  const slugTexto = `${n.eventSlug || ""} ${n.slug || ""}`;
  if (ESPORT.test(slugTexto)) return true;
  const t = `${n.titulo || ""} ${n.slug || ""}`.replace(/[-_]+/g, " ");
  return /\b(?:map\s*#?\s*\d{1,2}|mapa\s*#?\s*\d{1,2}|total\s+(?:de\s+)?maps?|maps?\s+(?:over|under)|map\s+handicap|mapas?\s+handicap|first\s+blood|total\s+rounds?|kills?|bo[1-7])\b/i.test(t);
}
const listaDe = body => body && Array.isArray(body.data) ? body.data : Array.isArray(body) ? body : [];

function montarUrlTrades(w, start, end, cursor) {
  const u = new URL(`${DATA_API}/v2/trades`);
  u.searchParams.set("user", w);
  u.searchParams.set("limit", String(LIMITE_PAGINA));
  u.searchParams.set("start", String(start));
  u.searchParams.set("end", String(end));
  u.searchParams.set("taker_only", "false");
  if (cursor) u.searchParams.set("cursor", cursor);
  return u.toString();
}

async function fetchComTimeout(url, init = {}, timeoutMs = TIMEOUT_FETCH_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort("tempo limite da requisição"), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}
async function fetchDados(url, init = {}) {
  let ultimaFalha;
  for (let tentativa = 0; tentativa < 2; tentativa++) {
    try {
      contar("polymarket");
      const r = await fetchComTimeout(url, init);
      if (r.status === 429) return r;
      if (r.status >= 500 && tentativa === 0) {
        await pausa(350);
        continue;
      }
      return r;
    } catch (e) {
      ultimaFalha = e;
      if (tentativa === 0) await pausa(250);
    }
  }
  throw ultimaFalha || new Error("falha de rede");
}

async function mapLimit(itens, limite, fn) {
  let i = 0;
  const workers = Array.from({ length: Math.min(limite, itens.length) }, async () => {
    while (i < itens.length) {
      const atual = i++;
      await fn(itens[atual]);
    }
  });
  await Promise.all(workers);
}

const NAO_TIME = /^(yes|no|over|under|sim|nao|map|mapa|game|total|winner|handicap)$/i;
const JOGO = /^(counter strike.*|cs2?|csgo|dota.*|league of legends|lol|valorant|rocket league|rainbow six.*|r6.*|call of duty|cod|overwatch.*|starcraft.*|mobile legends|mlbb|esports?)$/i;
const limparTime = v => String(v || "").replace(/\([^)]*\)/g, " ").replace(/\s+/g, " ").replace(/^[\s:|–—-]+|[\s:|–—-]+$/g, "").trim();

function extrairTimes(texto) {
  const s = String(texto || "").replace(/\s+/g, " ").trim();
  const m = s.match(/^(.+?)\s+(?:vs\.?|versus)\s+(.+)$/i);
  if (!m) return null;
  const antesDoVs = m[1].replace(/^.*:\s*/, "");
  const depoisDoVs = m[2].split(/\s+[-–—|]\s+/)[0].split(/\s+(?=(?:map|mapa|game|jogo|winner|total|handicap|over|under|moneyline|bo\s*\d)\b)/i)[0];
  const a = limparTime(antesDoVs);
  const b = limparTime(depoisDoVs);
  if (!a || !b || a.length > 50 || b.length > 50) return null;
  if (NAO_TIME.test(chaveTexto(a)) || NAO_TIME.test(chaveTexto(b))) return null;
  if (JOGO.test(chaveTexto(a)) || JOGO.test(chaveTexto(b))) return null;
  return { a, b };
}

const alvoMercado = n => `${n.titulo || ""} ${String(n.slug || "").replace(/[-_]+/g, " ")}`.replace(/\s+/g, " ").trim();
const RE_TOTAL_MAPAS = /\btotal\s*(?:(?:de|of)\s*)?(?:maps?|mapas)\b|\b(?:maps?|mapas)\s*(?:totais?|total|over|under|o\s*\/\s*u)\b|\b(?:over\s*\/\s*under|o\s*\/\s*u|over|under)\s*(?:\d+(?:[.,]\d+)?)?\s*(?:(?:de|of)\s*)?(?:maps?|mapas)\b|\b(?:number\s+of\s+maps|maps?\s+played)\b/i;

function numeroMapaDe(texto) {
  const m = String(texto || "").match(/\b(?:map|mapa|game|jogo)\s*#?\s*(\d{1,2})\b/i);
  return m ? Number(m[1]) : 0;
}
function classificarMercado(n) {
  const alvo = alvoMercado(n);
  const limpo = alvo.replace(/(\d)pt(\d)/gi, "$1.$2");
  const numMapa = numeroMapaDe(limpo);

  if (RE_TOTAL_MAPAS.test(limpo)) return { tipo: "total_mapas", rotulo: "Total de mapas" };
  if (/\b(handicap|spread)\b/i.test(limpo)) {
    return { tipo: "handicap", rotulo: numMapa ? `Handicap do mapa ${numMapa}` : "Handicap da partida" };
  }
  if (/\b(correct\s+score|exact\s+score|placar\s+correto|series\s+score|map\s+score)\b/i.test(limpo)) {
    return { tipo: "placar", rotulo: numMapa ? `Placar do mapa ${numMapa}` : "Placar correto" };
  }
  if (/\b(first\s+blood|primeiro\s+sangue)\b/i.test(limpo)) return { tipo: "first_blood", rotulo: "First Blood" };
  if (/\b(total\s+)?kills?\b|\btotal\s+(?:tower|dragon|objective)\b|\btotal\s+rounds?\b|\brounds?\s+(?:over|under)\b|\btotal\s+points?\b/i.test(limpo)) {
    let rotulo = /kills?/i.test(limpo) ? "Kills" : /rounds?/i.test(limpo) ? "Total de rodadas" : /tower|dragon|objective/i.test(limpo) ? "Objetivo da partida" : "Outro mercado";
    if (numMapa) rotulo += ` · mapa ${numMapa}`;
    return { tipo: "outro", rotulo };
  }
  if (numMapa) return { tipo: "mapa", rotulo: `Mapa ${numMapa}` };

  const temTimes = !!extrairTimes(n.titulo);
  if (temTimes || /\b(match\s+winner|winner\s+of\s+(?:the\s+)?match|match\s+result|moneyline|series\s+winner|who\s+will\s+win|win\s+(?:the\s+)?match|vencedor\s+da\s+partida)\b/i.test(limpo)) {
    return { tipo: "vencedor", rotulo: "Vencedor da partida" };
  }
  return { tipo: "outro", rotulo: truncar(n.titulo || n.slug || "Mercado não classificado", 65) };
}

function linhaTotal(n) {
  let limpo = `${n.titulo || ""} ${String(n.slug || "").replace(/\d{4}-\d{2}-\d{2}/g, " ")}`;
  limpo = limpo.replace(/[-_]+/g, " ").replace(/(\d)pt(\d)/gi, "$1.$2");
  limpo = limpo.replace(/\bbo\s*\d+\b/gi, " ");
  limpo = limpo.replace(/\b(?:map|mapa|maps|mapas|game|jogo)\s*#?\s*\d{1,2}\b/gi, " ");
  const decimal = limpo.match(/\b\d{1,2}[.,]\d+\b/);
  if (decimal) return decimal[0].replace(".", ",");
  const inteiro = limpo.match(/\b\d{1,2}\b/);
  return inteiro ? inteiro[0] : "";
}
function descreverAposta(n) {
  const c = classificarMercado(n);
  const o = String(n.outcome || "?").trim();
  const ol = o.toLowerCase();
  let escolha = o;
  if (ol === "yes") escolha = "Sim";
  else if (ol === "no") escolha = "Não";
  else if (c.tipo === "total_mapas" && /^(over|under)(?:\s+\d+(?:[.,]\d+)?)?$/i.test(o)) {
    const resultado = o.match(/^(over|under)(?:\s+(\d+(?:[.,]\d+)?))?$/i);
    const linha = (resultado && resultado[2] ? resultado[2].replace(".", ",") : "") || linhaTotal(n);
    escolha = `${resultado && resultado[1].toLowerCase() === "over" ? "Mais de" : "Menos de"}${linha ? ` ${linha}` : " (linha não identificada)"}`;
  } else if (c.tipo === "handicap") {
    let m = null;
    for (const x of String(n.titulo || "").matchAll(/:\s*([^:()]+?)\s*\(([+-]?\d+(?:[.,]\d+)?)\)/g)) m = x;
    if (m) {
      const valor = Number(m[2].replace(",", "."));
      const linha = chaveTexto(m[1]) === chaveTexto(o) ? valor : -valor;
      escolha = `${o} (${linha > 0 ? "+" : ""}${String(linha).replace(".", ",")})`;
    }
  }
  return { classificacao: c, escolha, texto: `${c.rotulo} — ${escolha}` };
}

function dataDoSlug(s) {
  const m = String(s || "").match(/(?:^|[^\d])(\d{4})-(\d{2})-(\d{2})(?:[^\d]|$)/);
  return m ? `${m[1]}-${m[2]}-${m[3]}` : null;
}
function extrairEvento(ev, cond) {
  const mercados = Array.isArray(ev && ev.markets) ? ev.markets : [];
  const mercado = mercados.find(m => m && String(m.conditionId || m.condition_id || "").toLowerCase() === cond);
  const candidatosHorario = [
    mercado && mercado.gameStartTime, mercado && mercado.game_start_time,
    ev && ev.gameStartTime, ev && ev.game_start_time,
    mercado && mercado.startTime, mercado && mercado.start_time,
    ev && ev.startTime, ev && ev.start_time,
    mercado && mercado.startDate, mercado && mercado.start_date,
    ev && ev.startDate, ev && ev.start_date,
  ];
  const inicioMs = candidatosHorario.map(paraMsHorario).find(v => v > 0) || 0;
  const titulo = String((ev && (ev.title || ev.name)) || "");
  const slug = String((ev && ev.slug) || "");
  const dataForte = dataDoSlug(slug);
  const dataPorCampo = [
    mercado && mercado.gameStartTime, mercado && mercado.game_start_time,
    ev && ev.gameStartTime, ev && ev.game_start_time,
    mercado && mercado.startDate, mercado && mercado.start_date,
    ev && ev.startDate, ev && ev.start_date,
  ].map(dataValor).find(Boolean);
  return {
    titulo, slug,
    data: dataForte || dataPorCampo || (inicioMs ? diaBR(inicioMs) : null),
    inicioMs,
    times: extrairTimes(titulo),
  };
}

// Em DRY_RUN, registra a chamada e devolve IDs sintéticos (não fala com o Telegram).
// Os IDs falsos ficam acima de ID_FALSO_MIN: quando o DRY_RUN é desligado, limparIdsFalsos()
// descarta tudo que foi guardado com eles (tópicos, placares, resumo, lista).
let DRY_SEQ = ID_FALSO_MIN;
async function enviarComRetentativa(env, metodo, corpo, tentativas = 3) {
  if (dryRun(env)) {
    DRY_SEQ++;
    contar("dryRunTelegram");
    console.log(`[DRY_RUN] ${metodo} ${JSON.stringify(corpo).slice(0, 800)}`);
    return {
      ok: true, status: 200, dry: true,
      result: { message_id: DRY_SEQ, message_thread_id: DRY_SEQ },
    };
  }
  let ultima = { ok: false, status: 0, desc: "falha desconhecida" };
  for (let i = 0; i < tentativas; i++) {
    try {
      contar("telegram");
      const r = await fetchComTimeout(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${metodo}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(corpo),
      });
      const j = await r.json().catch(() => ({}));
      if (j.ok) return { ok: true, result: j.result, status: r.status };
      ultima = { ok: false, status: r.status, desc: j.description || `HTTP ${r.status}` };
      if (r.status === 429) {
        const espera = Number(j.parameters && j.parameters.retry_after) || 5;
        ultima.retryAfter = espera;
        if (espera > ESPERA_MAX_429_S || i === tentativas - 1) return ultima;
        await pausa(espera * 1000);
        continue;
      }
      if (r.status >= 500 && i < tentativas - 1) {
        await pausa(400 * (i + 1));
        continue;
      }
      return ultima;
    } catch (e) {
      ultima = { ok: false, status: 0, desc: String((e && e.message) || e) };
      if (i < tentativas - 1) await pausa(300 * (i + 1));
    }
  }
  return ultima;
}

function nomeCarteira(cfg, w) {
  const c = cfg.carteiras.find(x => x.w === w);
  return (c && c.nome) || w.slice(0, 8) + "…";
}

async function garantirTopicoAtualizacoes(env) {
  const raw = await kvGet(env, "topico_atualizacoes");
  const salvo = Number(raw);
  // ID falso (sobra de DRY_RUN) é ignorado: o tópico é recriado de verdade.
  if (raw && salvo > 0 && (dryRun(env) || !idFalso(salvo))) return salvo;
  const r = await enviarComRetentativa(env, "createForumTopic", {
    chat_id: env.TELEGRAM_CHAT_ID,
    name: NOME_TOPICO_ATUALIZACOES,
  });
  if (!r.ok || !r.result || !r.result.message_thread_id) {
    console.warn("falha ao criar tópico Atualizações: " + limpar(env, r.desc || r.status));
    return 0;
  }
  const id = r.result.message_thread_id;
  if (!r.dry) await kvPut(env, "topico_atualizacoes", String(id)); // nunca grava ID falso
  return id;
}

// Divide a lista em blocos de até MAX_LISTA_CHARS (endereço abreviado ≈ 60 caracteres por carteira).
// Cada bloco leva cabeçalho (com "pág. i/N" quando há mais de um) e rodapé.
function blocosLista(cfg) {
  const total = cfg.carteiras.length;
  const rodape = `<code>${VERSAO}</code> · atualizado ${horaSegBR(Date.now())}`;
  if (!total) {
    return [`📋 <b>CARTEIRAS MONITORADAS</b> · 0\n\n<i>Nenhuma carteira no momento.</i>\n\n${rodape}`];
  }
  const linhas = cfg.carteiras.map((c, i) => {
    const nome = c.nome ? esc(c.nome) : "<i>sem nome</i>";
    return `<b>${String(i + 1).padStart(2, "0")}.</b> ${nome}\n<code>${abrevW(c.w)}</code>`;
  });
  const reserva = 140 + rodape.length; // espaço para cabeçalho + paginação + rodapé
  const grupos = [];
  let atual = [];
  let tam = 0;
  for (const linha of linhas) {
    const acresc = linha.length + 2;
    if (atual.length && tam + acresc > MAX_LISTA_CHARS - reserva) {
      grupos.push(atual);
      atual = [];
      tam = 0;
    }
    atual.push(linha);
    tam += acresc;
  }
  if (atual.length) grupos.push(atual);
  return grupos.map((ls, i) => {
    const pag = grupos.length > 1 ? ` · pág. ${i + 1}/${grupos.length}` : "";
    return `📋 <b>CARTEIRAS MONITORADAS</b> · ${total}${pag}\n\n` + ls.join("\n\n") + `\n\n${rodape}`;
  });
}

async function lerLista(env) {
  const raw = await kvGet(env, KV_LISTA);
  try {
    const v = raw ? JSON.parse(raw) : null;
    if (v && typeof v === "object") {
      const bruto = Array.isArray(v.msgIds)
        ? v.msgIds
        : (Number(v.msgId) > 0 ? [v.msgId] : []); // formato antigo (um bloco só)
      const ids = bruto.map(Number).filter(n => n > 0);
      const temFalso = !dryRun(env) && ids.some(idFalso);
      // IDs falsos de DRY_RUN: descarta tudo e força o reenvio da lista.
      if (temFalso) return { msgIds: [], hash: "", dia: "" };
      return { msgIds: ids, hash: String(v.hash || ""), dia: String(v.dia || "") };
    }
  } catch (_) { /* cai no padrão abaixo */ }
  return { msgIds: [], hash: "", dia: "" };
}

// Edita cada bloco no lugar; cria o que faltar; apaga os que sobraram quando a lista encolhe.
// Se uma falha interromper no meio depois de criar mensagens novas, guarda o que já existe
// (com hash vazio, para tentar de novo na próxima rodada) em vez de deixar mensagens órfãs.
async function atualizarListaCarteiras(env, cfg, forcar = false) {
  const hashAtual = cfg.carteiras.map(c => c.w + "|" + (c.nome || "")).join(",");
  const diaHoje = diaBR(Date.now());
  const lista = await lerLista(env);
  if (!forcar && lista.hash === hashAtual && lista.dia === diaHoje) return false;
  const topicoId = await garantirTopicoAtualizacoes(env);
  if (!topicoId) return false;

  const blocos = blocosLista(cfg);
  const antigos = lista.msgIds;
  const novos = [];
  let criouNovo = false;
  let falhou = false;

  for (let i = 0; i < blocos.length; i++) {
    const corpoEnvio = {
      chat_id: env.TELEGRAM_CHAT_ID, text: blocos[i], parse_mode: "HTML",
      disable_web_page_preview: true, message_thread_id: topicoId,
    };
    const id = antigos[i];
    if (id) {
      const edit = await enviarComRetentativa(env, "editMessageText", {
        chat_id: env.TELEGRAM_CHAT_ID, message_id: id, text: blocos[i],
        parse_mode: "HTML", disable_web_page_preview: true,
      });
      if (edit.ok || /not[ _]modified/i.test(edit.desc || "")) { novos.push(id); continue; }
      if (!/message to edit not found|message_id_invalid|can't be edited/i.test(edit.desc || "")) {
        console.warn(`lista bloco ${i + 1} (edit): ` + limpar(env, edit.desc).slice(0, 160));
        falhou = true;
        break;
      }
      // mensagem apagada no Telegram: cai no envio de uma nova
    }
    const novo = await enviarComRetentativa(env, "sendMessage", corpoEnvio);
    if (!novo.ok || !novo.result) {
      console.warn(`lista bloco ${i + 1} (send): ` + limpar(env, novo.desc).slice(0, 160));
      falhou = true;
      break;
    }
    novos.push(novo.result.message_id);
    criouNovo = true;
    if (i < blocos.length - 1) await pausa(150); // espaça envios para não bater no limite do grupo
  }

  if (falhou) {
    if (criouNovo) {
      const restantes = antigos.slice(novos.length);
      await kvPut(env, KV_LISTA, JSON.stringify({ msgIds: [...novos, ...restantes], hash: "", dia: "" }));
    }
    return false;
  }

  await kvPut(env, KV_LISTA, JSON.stringify({ msgIds: novos, hash: hashAtual, dia: diaHoje }));

  // Lista encolheu: apaga os blocos que sobraram (best-effort; a lista já foi gravada).
  for (let i = blocos.length; i < antigos.length; i++) {
    const r = await enviarComRetentativa(env, "deleteMessage", { chat_id: env.TELEGRAM_CHAT_ID, message_id: antigos[i] });
    if (!r.ok) console.warn("apagar bloco antigo da lista: " + limpar(env, r.desc).slice(0, 120));
  }
  return true;
}

function registrarAposta(estado, g) {
  const dia = diaBR(Date.now());
  if (!estado.resumo || estado.resumo.dia !== dia) {
    estado.resumo = { dia, apostas: {}, nomes: {}, msgId: null, mudou: true };
  }
  const r = estado.resumo;
  for (const it of g.itens) r.apostas[it.w] = (r.apostas[it.w] || 0) + 1;
  r.mudou = true;
}

async function atualizarResumo(env, estado, cfg) {
  const r = estado.resumo;
  if (!r || !r.mudou) return false;
  const linhas = Object.keys(r.apostas || {}).sort().map((w, i) => {
    const nome = (r.nomes && r.nomes[w]) || nomeCarteira(cfg, w);
    return `👤 <b>${String(i + 1).padStart(2, "0")} · ${esc(nome)}:</b> ${r.apostas[w]} aposta(s)`;
  });
  const total = Object.values(r.apostas || {}).reduce((a, b) => a + b, 0);
  const texto = `📊 <b>RESUMO DIÁRIO · ${diaComAnoBR(r.dia)}</b>\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${linhas.length ? linhas.join("\n") : "— nenhuma aposta —"}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `💰 <b>TOTAL:</b> ${total} aposta(s)\n\n` +
    `<i>Atualizado às ${horaBR(Date.now())} · ${VERSAO}</i>`;
  const corpo = { chat_id: env.TELEGRAM_CHAT_ID, text: texto, parse_mode: "HTML", disable_web_page_preview: true };
  if (r.msgId) {
    const e = await enviarComRetentativa(env, "editMessageText", { ...corpo, message_id: r.msgId });
    if (e.ok || /not[ _]modified/i.test(e.desc || "")) { r.mudou = false; return true; }
    if (/not found|can't be edited|message_id_invalid|message to edit/i.test((e.desc || "").toLowerCase())) r.msgId = null;
    else { console.warn("resumo diário: " + limpar(env, e.desc).slice(0, 160)); return false; }
  }
  const topicoResumo = Number(env.TELEGRAM_TOPICO_RESUMO) || 0;
  let novo = await enviarComRetentativa(env, "sendMessage", topicoResumo ? { ...corpo, message_thread_id: topicoResumo } : corpo);
  if (!novo.ok && topicoResumo && novo.status === 400 && /thread|topic/i.test(novo.desc || "")) {
    novo = await enviarComRetentativa(env, "sendMessage", corpo);
  }
  if (novo.ok && novo.result) { r.msgId = novo.result.message_id; r.mudou = false; return true; }
  console.warn("resumo diário não atualizado: " + limpar(env, novo.desc).slice(0, 160));
  return false;
}

async function lerConfig(env) {
  const raw = await kvGet(env, KV_CFG);
  let v = {};
  try { v = raw ? JSON.parse(raw) : {}; } catch (_) { throw new Error("configuração CFG corrompida"); }
  const lista = Array.isArray(v.carteiras) ? v.carteiras : [];
  const carteiras = [];
  const vistos = new Set();
  for (const c of lista) {
    const w = String((c && c.w) || "").trim().toLowerCase();
    if (!/^0x[a-f0-9]{40}$/.test(w) || vistos.has(w)) continue;
    vistos.add(w);
    carteiras.push({ w, nome: String((c && c.nome) || "").trim().slice(0, 40) });
  }
  return { carteiras, atualizado: typeof v.atualizado === "string" ? v.atualizado : null };
}
async function versaoAtual(env) {
  const raw = await kvGet(env, KV_CFG);
  if (!raw) return null;
  try { const v = JSON.parse(raw); return typeof v.atualizado === "string" ? v.atualizado : null; } catch (_) { return null; }
}
async function salvarCarteiras(env, carteiras, base) {
  if (!Array.isArray(carteiras)) throw new Error('payload inválido: esperado { "carteiras": [{ "w": "0x...", "nome": "..." }] }');
  const vistos = new Set();
  const lista = [];
  let ignoradas = 0;
  for (const c of carteiras) {
    const w = String((c && c.w) || "").trim().toLowerCase();
    if (!/^0x[a-f0-9]{40}$/.test(w) || vistos.has(w)) { ignoradas++; continue; }
    vistos.add(w);
    lista.push({ w, nome: String((c && c.nome) || "").trim().slice(0, 40) });
  }
  if (base !== undefined && base !== await versaoAtual(env)) {
    const erro = new Error("lista alterada em outro aparelho");
    erro.conflito = true;
    throw erro;
  }
  const atualizado = new Date().toISOString();
  await kvPut(env, KV_CFG, JSON.stringify({ carteiras: lista, atualizado }));
  return { salvas: lista.length, ignoradas, atualizado };
}

async function migrarFilaAntiga(env) {
  try {
    const raw = await kvGet(env, KV_ESTADO_ANTIGO);
    if (!raw) return [];
    const a = JSON.parse(raw);
    const lista = [a.pendentes, a.pending, a.fila, a.queue].find(Array.isArray) || [];
    const out = [];
    for (const item of lista) {
      const n = normalizar((item && (item.t || item.trade)) || item);
      if (!n || !n.cond || !eEsports(n)) continue;
      const w = String((item && item.c && item.c.w) || (item && item.w) || n.w).toLowerCase();
      if (!/^0x[a-f0-9]{40}$/.test(w)) continue;
      out.push({ id: `${w}|${chaveTrade(n)}`, w, n, tent: 0, nextTryAt: 0 });
    }
    return out;
  } catch (e) {
    console.warn("migração de fila antiga: " + limpar(env, e).slice(0, 120));
    return [];
  }
}

// Tópicos da versão antiga (chave "topicos"): lidos só uma vez, na migração.
async function lerTopicosLegado(env) {
  const raw = await kvGet(env, KV_TOPICOS_LEGADO);
  if (!raw) return {};
  let v;
  try { v = JSON.parse(raw); } catch (_) { return null; }
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const out = {};
  for (const [k, val] of Object.entries(v)) {
    if (typeof val === "number" || /^\d+$/.test(String(val))) {
      out[k] = { id: Number(val), nome: "", inicioMs: 0, dia: "", resumo: { msgId: null, mercados: {} } };
    } else if (val && typeof val === "object" && Number(val.id) > 0) {
      out[k] = {
        id: Number(val.id),
        nome: String(val.nome || ""),
        inicioMs: Number(val.inicioMs || 0),
        dia: String(val.dia || ""),
        resumo: val.resumo && typeof val.resumo === "object" ? val.resumo : { msgId: null, mercados: {} },
      };
    }
  }
  return out;
}

async function lerEstado(env) {
  const raw = await kvGet(env, KV_ESTADO);
  let e;
  if (raw) {
    try { e = JSON.parse(raw); } catch (_) { throw new Error("estado4 corrompido; interrompi para não sobrescrever a fila"); }
  } else {
    e = { pendentes: await migrarFilaAntiga(env), vistos: {}, ultimo: {}, coletas: {}, conhecidas: {}, resumo: null, eventos: {} };
  }
  delete e.indiceCarteira; // campos de versões anteriores
  delete e.lista;
  e.pendentes = Array.isArray(e.pendentes) ? e.pendentes.filter(it => it && it.n && it.w) : [];
  e.vistos = e.vistos && typeof e.vistos === "object" ? e.vistos : {};
  e.ultimo = e.ultimo && typeof e.ultimo === "object" ? e.ultimo : {};
  e.coletas = e.coletas && typeof e.coletas === "object" ? e.coletas : {};
  e.conhecidas = e.conhecidas && typeof e.conhecidas === "object" ? e.conhecidas : {};
  e.resumo = e.resumo && typeof e.resumo === "object" ? e.resumo : null;
  e.eventos = e.eventos && typeof e.eventos === "object" && !Array.isArray(e.eventos) ? e.eventos : {};
  return e;
}

async function garantirTopicos(env, estado) {
  if (estado.topicos && typeof estado.topicos === "object" && !Array.isArray(estado.topicos)) return;
  const legado = await lerTopicosLegado(env);
  if (legado === null) throw new Error("registro de tópicos ilegível; rodada interrompida para evitar duplicações");
  estado.topicos = legado;
}

// Com o DRY_RUN desligado, descarta tudo que foi guardado com IDs falsos:
// tópicos de teste somem (serão criados de verdade quando houver aviso) e msgIds de placar/resumo zeram.
function limparIdsFalsos(estado) {
  let n = 0;
  for (const [k, t] of Object.entries(estado.topicos || {})) {
    if (t && idFalso(t.id)) { delete estado.topicos[k]; n++; }
    else if (t && t.resumo && idFalso(t.resumo.msgId)) { t.resumo.msgId = null; n++; }
  }
  if (estado.resumo && idFalso(estado.resumo.msgId)) {
    estado.resumo.msgId = null;
    estado.resumo.mudou = true;
    n++;
  }
  return n;
}

// Retrato do estado sem os campos que mudam a cada rodada (cursor e horário do último save).
// Se o retrato não mudou, não há o que gravar.
function semCheckpoint(e) {
  const { ultimo, salvoEm, pendentes, ...resto } = e;
  return JSON.stringify(resto) + "|" + JSON.stringify((pendentes || []).map(it => [it.id, it.tent]));
}
function precisaGravar(estado, base) {
  return semCheckpoint(estado) !== base || Date.now() - Number(estado.salvoEm || 0) > CHECKPOINT_MS;
}

// Até onde a deduplicação precisa olhar: trades anteriores a esse ponto não voltam numa busca futura.
function corteVistos(estado, w) {
  if (estado.ultimo[w]) return Number(estado.ultimo[w]) - SOBREPOSICAO_S;
  const job = estado.coletas[w];
  return job && job.start ? Number(job.start) : 0;
}

function podarEstado(estado) {
  const pendentes = new Set(estado.pendentes.map(it => it.id));
  for (const w of Object.keys(estado.vistos)) {
    const lista = Array.isArray(estado.vistos[w]) ? estado.vistos[w] : [];
    const corte = corteVistos(estado, w);
    const mantidos = lista.filter(k => tsDaChave(k) >= corte || pendentes.has(`${w}|${k}`));
    if (mantidos.length) estado.vistos[w] = mantidos;
    else delete estado.vistos[w];
  }
  const limite = diaBR(Date.now() - DIAS_TOPICO * 86400000);
  for (const chave of Object.keys(estado.topicos)) {
    const t = estado.topicos[chave];
    if (!t || (t.dia && t.dia < limite)) delete estado.topicos[chave];
  }
  for (const cond of Object.keys(estado.eventos)) {
    if (!estado.topicos[estado.eventos[cond]]) delete estado.eventos[cond];
  }
}

async function gravar(env, estado) {
  podarEstado(estado);
  estado.salvoEm = Date.now();
  await kvPut(env, KV_ESTADO, JSON.stringify(estado));
}

// Segunda gravação: se falhar por erro passageiro, tenta mais uma vez.
async function gravarComNovaTentativa(env, estado) {
  try {
    await gravar(env, estado);
  } catch (e) {
    await pausa(400);
    await gravar(env, estado);
  }
}

// Rodízio pelo relógio: não grava nada. A cada minuto a janela de 20 carteiras anda.
function selecionarCarteiras(carteiras) {
  if (carteiras.length <= CARTEIRAS_POR_CICLO) return carteiras;
  const inicio = (Math.floor(Date.now() / 60000) * CARTEIRAS_POR_CICLO) % carteiras.length;
  return Array.from({ length: CARTEIRAS_POR_CICLO }, (_, i) => carteiras[(inicio + i) % carteiras.length]);
}

// De onde começar a buscar os trades de uma carteira.
function inicioColeta(estado, w, agora) {
  const ultimo = Number(estado.ultimo[w] || 0);
  if (ultimo) return Math.max(ultimo - SOBREPOSICAO_S, agora - JANELA_MAXIMA_S); // continuidade
  if (estado.conhecidas[w]) return agora - SOBREPOSICAO_S;                       // reincluída: do agora
  return agora - JANELA_INICIAL_S;                                               // carteira nova
}

async function coletarCarteira(c, estado, pendPorW) {
  const w = c.w;
  const agora = agoraS();
  const existentes = new Set(Array.isArray(estado.vistos[w]) ? estado.vistos[w] : []);
  for (const k of pendPorW.get(w) || []) existentes.add(k);

  let job = estado.coletas[w];
  if (!job || typeof job !== "object") {
    const inicio = inicioColeta(estado, w, agora);
    job = { start: Math.max(1, inicio), end: agora, cursor: null, falhas: 0 };
    estado.coletas[w] = job;
  }
  if (typeof job.falhas !== "number") job.falhas = 0;

  let novos = 0;
  let concluido = false;
  try {
    const cursorAntes = job.cursor || null;
    const r = await fetchDados(montarUrlTrades(w, job.start, job.end, cursorAntes), { headers: { Accept: "application/json" } });
    if (!r.ok) {
      throw new Error(`Data API HTTP ${r.status}${r.status === 429 ? " (limite de chamadas; cursor preservado)" : ""}`);
    }
    const body = await r.json();
    let filaCheia = false;
    for (const bruto of listaDe(body)) {
      const n = normalizar(bruto);
      if (!n || !n.cond || !eEsports(n)) continue;
      const k = chaveTrade(n);
      if (existentes.has(k)) continue;
      if (estado.pendentes.length >= MAX_PENDENTES) { filaCheia = true; break; }
      existentes.add(k);
      estado.pendentes.push({ id: `${w}|${k}`, w, n, tent: 0, nextTryAt: 0 });
      novos++;
    }
    const next = body && body.pagination ? body.pagination.next_cursor : null;
    job.falhas = 0; // sucesso: zera contador
    if (filaCheia) {
      job.cursor = cursorAntes;   // retoma esta mesma página na próxima rodada
    } else if (next) {
      job.cursor = String(next);  // ainda há páginas: continua na próxima rodada
    } else {
      concluido = true;
      delete estado.coletas[w];
      estado.ultimo[w] = job.end;
      // "conhecida" só depois da primeira coleta completa: se as primeiras tentativas falharem,
      // a carteira nova ainda recupera a janela inicial de 1 h.
      if (!estado.conhecidas[w]) estado.conhecidas[w] = agora;
    }
  } catch (e) {
    job.falhas = (job.falhas || 0) + 1;
    if (job.falhas >= FALHAS_COLETA_RESET) {
      // Cursor/job preso: abandona e deixa a próxima rodada recomeçar a partir de ultimo[w].
      console.warn(`[${w.slice(0, 8)}] reset coleta após ${job.falhas} falhas: ${String((e && e.message) || e).slice(0, 140)}`);
      delete estado.coletas[w];
    } else {
      estado.coletas[w] = job;
    }
    if (existentes.size) estado.vistos[w] = [...existentes];
    else delete estado.vistos[w];
    throw e;
  }
  if (existentes.size) estado.vistos[w] = [...existentes];
  else delete estado.vistos[w];
  if (!concluido && estado.coletas[w]) estado.coletas[w] = job;
  return { novos };
}

async function coletar(cfg, estado, expirou) {
  const alvos = selecionarCarteiras(cfg.carteiras);
  const pendPorW = new Map();
  for (const it of estado.pendentes) {
    if (!pendPorW.has(it.w)) pendPorW.set(it.w, []);
    pendPorW.get(it.w).push(it.id.slice(it.w.length + 1));
  }
  let novos = 0, erros = 0, puladas = 0;
  await mapLimit(alvos, CONCORRENCIA, async c => {
    if (expirou()) { puladas++; return; }
    try {
      novos += (await coletarCarteira(c, estado, pendPorW)).novos;
    } catch (e) {
      erros++;
      console.warn(`[${c.w.slice(0, 8)}] ${String((e && e.message) || e).slice(0, 180)}`);
    }
  });
  return { novos, erros, puladas, consultadas: alvos.length - puladas };
}

// Carteira removida: some com a fila, o cursor e a deduplicação.
// Se for reincluída, volta como "reincluída" (ver inicioColeta), sem buscar a última hora de novo.
function podarCarteirasRemovidas(estado, cfg) {
  const ativas = new Set(cfg.carteiras.map(c => c.w));
  estado.pendentes = estado.pendentes.filter(it => ativas.has(it.w));
  for (const mapa of [estado.coletas, estado.vistos, estado.ultimo]) {
    for (const w of Object.keys(mapa)) if (!ativas.has(w)) delete mapa[w];
  }
}

function sincronizarResumo(estado, cfg) {
  const r = estado.resumo;
  if (!r || r.dia !== diaBR(Date.now()) || !r.apostas) return;
  let mudou = false;
  for (const w of Object.keys(r.apostas)) {
    if (!cfg.carteiras.some(c => c.w === w)) { delete r.apostas[w]; mudou = true; }
  }
  const nomes = Object.fromEntries(Object.keys(r.apostas).sort().map(w => [w, nomeCarteira(cfg, w)]));
  if (JSON.stringify(nomes) !== JSON.stringify(r.nomes || {})) { r.nomes = nomes; mudou = true; }
  if (mudou) r.mudou = true;
}

async function buscarEvento(env, n, cacheEventos) {
  const chaveCache = n.eventSlug || `cond:${n.cond}`;
  if (cacheEventos.has(chaveCache)) return cacheEventos.get(chaveCache);
  const tentar = async () => {
    let ev = null;
    try {
      if (n.eventSlug) {
        const u = new URL(`${GAMMA_API}/events`);
        u.searchParams.set("slug", n.eventSlug);
        contar("gamma");
        const r = await fetchComTimeout(u.toString(), { headers: { Accept: "application/json" } });
        if (r.ok) {
          const lista = await r.json();
          const arr = Array.isArray(lista) ? lista : (lista && Array.isArray(lista.data) ? lista.data : []);
          if (arr.length) ev = extrairEvento(arr[0], n.cond);
        }
      }
      if (!ev) {
        const u = new URL(`${GAMMA_API}/markets`);
        u.searchParams.set("condition_ids", n.cond);
        contar("gamma");
        const r = await fetchComTimeout(u.toString(), { headers: { Accept: "application/json" } });
        if (r.ok) {
          const lista = await r.json();
          const arr = Array.isArray(lista) ? lista : (lista && Array.isArray(lista.data) ? lista.data : []);
          const m = arr[0];
          if (m) {
            const pai = (Array.isArray(m.events) && m.events[0]) || m.event || { markets: [m], title: m.question, slug: m.slug };
            ev = extrairEvento(pai, n.cond);
          }
        }
      }
    } catch (e) {
      console.warn("buscarEvento: " + limpar(env, e).slice(0, 120));
    }
    return ev;
  };
  let ev = await tentar();
  if (!ev) { await pausa(400); ev = await tentar(); }
  cacheEventos.set(chaveCache, ev);
  return ev;
}

// Dados do evento guardados no tópico (cache): o que a mensagem precisa, nada além disso.
function snapshotEvento(ev) {
  if (!ev) return null;
  return {
    titulo: ev.titulo || "",
    slug: ev.slug || "",
    times: ev.times || null,
    inicioMs: ev.inicioMs || 0,
    data: ev.data || null,
  };
}

function novoTopico(id, nome, ev) {
  return {
    id, nome,
    inicioMs: (ev && ev.inicioMs) || 0,
    dia: (ev && ev.data) || diaBR(Date.now()),
    ev: snapshotEvento(ev),
    evAtualizadoEm: Date.now(),
    resumo: { msgId: null, mercados: {} },
  };
}

function nomeTopicoPartida(ev, n) {
  const times = (ev && ev.times) || extrairTimes(n && n.titulo);
  let base;
  if (times) base = `${times.a} vs ${times.b}`;
  else if (ev && ev.titulo) base = ev.titulo;
  else if (n && n.titulo) base = n.titulo;
  else base = "Partida";

  const ms = ev && ev.inicioMs;
  if (ms > 0) {
    const hora = horaBR(ms);
    const hoje = diaBR(ms) === diaBR(Date.now());
    base += hoje ? ` · ${hora}` : ` · ${diaCurtoBR(ms)} ${hora}`;
  } else if (ev && ev.data) {
    base += ` · ${diaComAnoBR(ev.data)}`;
  }
  return truncar(base, 128);
}

function textoGrupo(g, cfg, ev) {
  const nome = nomeCarteira(cfg, g.w);
  const n0 = g.itens[0] && g.itens[0].n;
  const titulo = ev && ev.times ? `${ev.times.a} vs ${ev.times.b}` : (ev && ev.titulo) || (n0 && n0.titulo) || "Partida";
  const quando = ev && ev.inicioMs ? `🕒 ${diaCurtoBR(ev.inicioMs)} · ${horaBR(ev.inicioMs)}` : ev && ev.data ? `📅 ${diaComAnoBR(ev.data)}` : "";

  const agregado = new Map();
  for (const it of g.itens) {
    const d = descreverAposta(it.n);
    const chave = `${d.classificacao.rotulo} - ${d.escolha}`;
    if (!agregado.has(chave)) {
      agregado.set(chave, { rotulo: d.classificacao.rotulo, escolha: d.escolha, sizeTotal: 0, valorTotal: 0, qtd: 0 });
    }
    const a = agregado.get(chave);
    const valor = Number(it.n.size || 0) * Number(it.n.price || 0);
    a.sizeTotal += Number(it.n.size || 0);
    a.valorTotal += valor;
    a.qtd++;
  }

  const corpo = [...agregado.values()].map(a => {
    const precoMedio = a.sizeTotal > 0 ? (a.valorTotal / a.sizeTotal) : 0;
    const notaExecucoes = a.qtd > 1 ? ` <i>(${a.qtd}x)</i>` : "";
    return `<b>${esc(a.rotulo)}</b>\n→ ${esc(a.escolha)}\n<i>${moeda(a.valorTotal)} · ${quantidade(a.sizeTotal)} cotas @ ${numeroBR(precoMedio, 3)} · ${pct(precoMedio)}</i>${notaExecucoes}`;
  }).join("\n\n");

  const linhas = [`🎯 <b>${esc(titulo)}</b>`];
  if (quando) linhas.push(quando);
  linhas.push("", `👤 <b>${esc(nome)}</b>`, "", corpo, "", `<i>${horaSegBR(Date.now())} · ${VERSAO}</i>`);
  return linhas.join("\n");
}

function agrupar(pendentes) {
  const agora = Date.now();
  const prontos = pendentes.filter(it => !it.nextTryAt || it.nextTryAt <= agora);
  const grupos = new Map();
  for (const it of prontos) {
    const chave = `${it.w}|${it.n.cond}`;
    if (!grupos.has(chave)) grupos.set(chave, { w: it.w, cond: it.n.cond, itens: [] });
    grupos.get(chave).itens.push(it);
  }
  return [...grupos.values()];
}

function registrarNoResumo(topico, g) {
  if (!topico.resumo) topico.resumo = { msgId: null, mercados: {} };
  for (const it of g.itens) {
    const d = descreverAposta(it.n);
    const chaveM = `${d.classificacao.rotulo.replace(" da partida", "")} - ${d.escolha}`;
    const valor = Number(it.n.size || 0) * Number(it.n.price || 0);
    if (!topico.resumo.mercados[chaveM]) topico.resumo.mercados[chaveM] = { valor: 0, qtd: 0 };
    topico.resumo.mercados[chaveM].valor += valor;
    topico.resumo.mercados[chaveM].qtd += 1;
  }
}

async function atualizarPlacarVivo(env, topico) {
  if (!topico.resumo || !topico.resumo.mercados) return;
  let totalGeral = 0;
  let totalApostas = 0;
  const entradas = Object.entries(topico.resumo.mercados)
    .sort((a, b) => b[1].valor - a[1].valor)
    .map(([mercado, dados]) => {
      totalGeral += Number(dados.valor || 0);
      totalApostas += Number(dados.qtd || 0);
      return `▪️ <b>${esc(mercado)}</b>\n   └ ${dados.qtd} aposta(s) · ${moeda(dados.valor)}`;
    }).join("\n\n");
  const texto = `📊 <b>PLACAR AO VIVO</b>\n` +
    `🎯 <b>${esc(topico.nome || "Partida")}</b>\n` +
    `━━━━━━━━━━━━━━━━━━━━\n\n` +
    `${entradas || "— nenhuma aposta registrada —"}\n\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `💰 <b>TOTAL GERAL:</b> ${moeda(totalGeral)} · ${totalApostas} aposta(s)\n` +
    `<i>${VERSAO}</i>`;
  if (topico.resumo.msgId) {
    const edit = await enviarComRetentativa(env, "editMessageText", {
      chat_id: env.TELEGRAM_CHAT_ID, message_id: topico.resumo.msgId, text: texto,
      parse_mode: "HTML", disable_web_page_preview: true,
    });
    if (edit.ok || /message is not modified|not[ _]modified/i.test(edit.desc || "")) return;
    if (/message to edit not found|message_id_invalid|can't be edited/i.test((edit.desc || "").toLowerCase())) topico.resumo.msgId = null;
    else { console.warn("Falha ao editar placar: " + limpar(env, edit.desc).slice(0, 160)); return; }
  }
  const novo = await enviarComRetentativa(env, "sendMessage", {
    chat_id: env.TELEGRAM_CHAT_ID, message_thread_id: topico.id, text: texto,
    parse_mode: "HTML", disable_web_page_preview: true,
  });
  if (novo.ok && novo.result) topico.resumo.msgId = novo.result.message_id;
  else console.warn("Falha ao enviar placar: " + limpar(env, novo.desc || novo.status).slice(0, 160));
}

// Funde o resumo de mercados de um tópico origem no destino (usado na migração cond → slug).
function fundirResumoTopico(destino, origem) {
  if (!origem || !origem.resumo || !origem.resumo.mercados) return;
  if (!destino.resumo) destino.resumo = { msgId: null, mercados: {} };
  for (const [k, v] of Object.entries(origem.resumo.mercados)) {
    if (!destino.resumo.mercados[k]) destino.resumo.mercados[k] = { valor: 0, qtd: 0 };
    destino.resumo.mercados[k].valor += Number(v.valor || 0);
    destino.resumo.mercados[k].qtd += Number(v.qtd || 0);
  }
  if (!destino.ev && origem.ev) destino.ev = origem.ev;
  if (!destino.inicioMs && origem.inicioMs) destino.inicioMs = origem.inicioMs;
  if (!destino.dia && origem.dia) destino.dia = origem.dia;
}

function reapontarEventos(estado, chaveAntiga, chaveNova) {
  for (const [cond, k] of Object.entries(estado.eventos)) {
    if (k === chaveAntiga) estado.eventos[cond] = chaveNova;
  }
}

async function enviarGrupo(env, g, cfg, estado, cacheEventos) {
  const n = g.itens[0].n;
  const tent = Math.max(...g.itens.map(it => Number(it.tent || 0)));
  const slugTrade = (n.eventSlug || "").trim().toLowerCase() || null;

  // 1) Tópico já conhecido via condition_id
  const chaveAtual = estado.eventos[n.cond] || null;
  let topico = chaveAtual && estado.topicos[chaveAtual] ? estado.topicos[chaveAtual] : null;
  let chaveEv = topico ? chaveAtual : null;

  // 2) Se o trade já traz eventSlug e existe tópico sob esse slug, preferir esse (evita tópico por mercado).
  if (slugTrade && estado.topicos[slugTrade]) {
    topico = estado.topicos[slugTrade];
    chaveEv = slugTrade;
  }

  // Dados do evento: cache no tópico; Gamma só quando o prazo vence.
  let ev = topico && topico.ev ? topico.ev : null;
  const validade = ev ? EV_VALIDADE_MS : EV_FALHA_MS;
  if (!topico || Date.now() - Number(topico.evAtualizadoEm || 0) > validade) {
    const buscado = await buscarEvento(env, n, cacheEventos);
    if (buscado) ev = snapshotEvento(buscado);
    if (topico) {
      topico.evAtualizadoEm = Date.now();
      if (buscado) topico.ev = ev;
    }
    if (!topico && !buscado && tent < TENTATIVAS_METADADOS) {
      return {
        ok: false,
        deferido: true,
        desc: `aguardando dados do evento [${tent + 1}/${TENTATIVAS_METADADOS}]`,
        retryAfter: RETRY_METADADOS_S,
      };
    }
  }

  // Chave canônica: sempre slug do evento quando existir. condition_id só como último recurso.
  const slugEvento = ((ev && ev.slug) || slugTrade || "").trim().toLowerCase() || null;
  const chaveCanon = slugEvento || n.cond;

  // 3) Migração: tópico criado sob condition_id (ou outra chave) → mover/fundir no slug do evento.
  if (topico && chaveEv && slugEvento && chaveEv !== slugEvento) {
    const origem = chaveEv;
    const destino = estado.topicos[slugEvento];
    if (destino && destino !== topico) {
      fundirResumoTopico(destino, topico);
      topico = destino; // mantém o tópico "certo"; o antigo deixa de ser referenciado
    } else {
      estado.topicos[slugEvento] = topico;
    }
    delete estado.topicos[origem];
    reapontarEventos(estado, origem, slugEvento);
    chaveEv = slugEvento;
  }

  if (!topico) {
    chaveEv = chaveCanon;
    if (estado.topicos[chaveEv]) {
      topico = estado.topicos[chaveEv];
    } else {
      const nome = nomeTopicoPartida(ev, n);
      const r = await enviarComRetentativa(env, "createForumTopic", {
        chat_id: env.TELEGRAM_CHAT_ID,
        name: nome,
      });
      if (!r.ok || !r.result || !r.result.message_thread_id) {
        return {
          ok: false,
          status: r.status || 0,
          desc: "falha ao criar tópico: " + (r.desc || ""),
          retryAfter: r.retryAfter || 0,
        };
      }
      topico = novoTopico(r.result.message_thread_id, nome, ev);
      estado.topicos[chaveEv] = topico;
    }
  }

  estado.eventos[n.cond] = chaveEv;

  const corpo = {
    chat_id: env.TELEGRAM_CHAT_ID, message_thread_id: topico.id,
    text: textoGrupo(g, cfg, ev), parse_mode: "HTML", disable_web_page_preview: true,
  };
  let r = await enviarComRetentativa(env, "sendMessage", corpo);
  if (!r.ok && r.status === 400 && /thread|topic|TOPIC_ID_INVALID|TOPIC_DELETED/i.test(r.desc || "")) {
    // tópico apagado no Telegram: cria outro, preserva o placar acumulado e reenvia
    const nome = nomeTopicoPartida(ev, n);
    const rec = await enviarComRetentativa(env, "createForumTopic", {
      chat_id: env.TELEGRAM_CHAT_ID,
      name: nome,
    });
    if (rec.ok && rec.result && rec.result.message_thread_id) {
      const mercadosAntigos = (topico.resumo && topico.resumo.mercados) || {};
      topico = novoTopico(rec.result.message_thread_id, nome, ev);
      topico.resumo.mercados = mercadosAntigos;
      estado.topicos[chaveEv] = topico;
      r = await enviarComRetentativa(env, "sendMessage", { ...corpo, message_thread_id: topico.id });
    }
  }
  if (r.ok && r.result) {
    // O aviso já saiu: falha no placar não pode fazer o aviso ser reenviado.
    try {
      registrarNoResumo(topico, g);
      await atualizarPlacarVivo(env, topico);
    } catch (e) {
      console.warn("placar: " + limpar(env, e).slice(0, 160));
    }
    return { ok: true };
  }
  return {
    ok: false, status: r.status || 0, desc: r.desc || `HTTP ${r.status}`,
    retryAfter: r.retryAfter || 0, permanente: r.status === 400,
  };
}

async function ciclo(env, origem) {
  const inicio = Date.now();
  const decorrido = () => Date.now() - inicio;
  CONTADOR = { polymarket: 0, gamma: 0, telegram: 0, dryRunTelegram: 0, kvLeitura: 0, kvGravacao: 0 };
  const resultado = {
    versao: VERSAO, origem, dryRun: dryRun(env), carteiras: 0, consultadas: 0, puladas: 0, novos: 0,
    enviados: 0, falhas: 0, descartados: 0, pendentes: 0, erros: [],
  };
  const faltam = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "CFG"].filter(k => !env[k]);
  if (faltam.length) {
    resultado.erros.push("faltam configurações: " + faltam.join(", "));
    return finalizar(resultado, inicio);
  }
  try {
    const cfg = await lerConfig(env);
    resultado.carteiras = cfg.carteiras.length;
    const estado = await lerEstado(env);
    const base = semCheckpoint(estado); // retrato antes de qualquer mudança desta rodada
    await garantirTopicos(env, estado);

    // DRY_RUN desligado: descarta IDs falsos que sobraram de testes.
    if (!dryRun(env)) {
      const limpos = limparIdsFalsos(estado);
      if (limpos) console.log(`IDs falsos de DRY_RUN descartados: ${limpos}`);
    }

    // Sem carteiras na configuração (ex.: chave apagada por engano): não mexe em nada.
    if (cfg.carteiras.length) {
      podarCarteirasRemovidas(estado, cfg);
      sincronizarResumo(estado, cfg);
    }
    try {
      await atualizarListaCarteiras(env, cfg);
    } catch (e) {
      resultado.erros.push("lista de carteiras: " + truncar(limpar(env, e), 160));
    }

    if (cfg.carteiras.length && estado.pendentes.length < MAX_PENDENTES) {
      const coleta = await coletar(cfg, estado, () => decorrido() > PRAZO_COLETA_MS);
      resultado.novos = coleta.novos;
      resultado.consultadas = coleta.consultadas;
      resultado.puladas = coleta.puladas;
      if (coleta.erros) resultado.erros.push(`${coleta.erros} carteira(s) não consultadas nesta rodada`);
    }

    // Grava ANTES de enviar. Se a gravação falhar (ex.: cota do dia esgotada), nada é enviado.
    const prontos = agrupar(estado.pendentes).slice(0, MAX_AVISOS);
    if (prontos.length || precisaGravar(estado, base)) await gravar(env, estado);

    const cacheEventos = new Map();
    const enviados = new Set();
    const descartados = new Set();
    let mudouEnvio = false;
    for (const g of prontos) {
      if (decorrido() > PRAZO_ENVIO_MS) break; // o restante fica na fila para a próxima rodada
      mudouEnvio = true;
      let r;
      try {
        r = await enviarGrupo(env, g, cfg, estado, cacheEventos);
      } catch (e) {
        r = { ok: false, status: 0, desc: truncar(limpar(env, e), 200) };
      }
      if (r.ok) {
        for (const it of g.itens) enviados.add(it.id);
        registrarAposta(estado, g);
        resultado.enviados++;
        continue;
      }
      const tentativa = Math.max(...g.itens.map(it => Number(it.tent || 0))) + 1;
      if (r.deferido) {
        // ainda sem dados do evento: espera e tenta de novo; não conta como falha do Telegram
        for (const it of g.itens) {
          it.tent = tentativa;
          it.nextTryAt = Date.now() + RETRY_METADADOS_S * 1000;
        }
        continue;
      }
      resultado.falhas++;
      const motivo = truncar(r.desc || `falha no Telegram (${r.status})`, 160);
      if (r.permanente || tentativa >= MAX_TENTATIVAS) {
        for (const it of g.itens) descartados.add(it.id);
        resultado.erros.push(`descartado após ${tentativa} tentativa(s): ${motivo}`);
      } else {
        const atraso = Math.max(r.retryAfter || 0, Math.min(600, 5 * 2 ** Math.min(tentativa, 7)));
        for (const it of g.itens) {
          it.tent = tentativa;
          it.nextTryAt = Date.now() + atraso * 1000;
        }
        resultado.erros.push(`nova tentativa em ${atraso}s: ${motivo}`);
      }
    }

    if (enviados.size || descartados.size) {
      estado.pendentes = estado.pendentes.filter(it => !enviados.has(it.id) && !descartados.has(it.id));
      mudouEnvio = true;
    }
    resultado.descartados = descartados.size;

    if (estado.resumo && estado.resumo.mudou) {
      try {
        if (await atualizarResumo(env, estado, cfg)) mudouEnvio = true;
      } catch (e) {
        resultado.erros.push("resumo: " + truncar(limpar(env, e), 160));
      }
    }
    resultado.pendentes = estado.pendentes.length;

    // Segunda gravação: tira da fila o que saiu e guarda tópicos, placares e tentativas.
    if (mudouEnvio) await gravarComNovaTentativa(env, estado);
  } catch (e) {
    resultado.erros.push(truncar(limpar(env, e), 300));
  }
  return finalizar(resultado, inicio);
}

// Fecha a rodada: duração e contadores. O cron grava uma linha no log; o /notify devolve na resposta.
function finalizar(resultado, inicio) {
  resultado.duracaoMs = Date.now() - inicio;
  Object.assign(resultado, CONTADOR || {});
  CONTADOR = null;
  if (resultado.origem === "cron") console.log(JSON.stringify(resultado));
  return resultado;
}

async function salvarCarteirasPayload(env, body) {
  return salvarCarteiras(env, body && body.carteiras, body && body.base);
}

// Depois de mudar a lista: atualiza só as mensagens da lista (a fila não é tocada).
async function atualizarListaAposConfig(env) {
  try {
    const cfg = await lerConfig(env);
    await atualizarListaCarteiras(env, cfg, true);
  } catch (e) {
    console.warn("falha ao atualizar a lista de carteiras: " + limpar(env, e));
  }
}

function autorizado(req, env, body) {
  if (!env.CHAVE) return false;
  const auth = req.headers.get("Authorization") || "";
  const token = auth.replace(/^Bearer\s+/i, "").trim() || (body && typeof body.chave === "string" ? body.chave : "");
  return token === env.CHAVE;
}

export default {
  async fetch(req, env) {
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    const path = new URL(req.url).pathname;

    if (req.method === "GET" && path === "/") {
      return textResp(`ok · ${VERSAO}${dryRun(env) ? " · DRY_RUN" : ""}`);
    }

    // Healthcheck para monitor externo: 503 se o último checkpoint tiver mais de 20 min.
    // Público de propósito (não exige CHAVE): só expõe idade do checkpoint e nº de pendentes.
    if (req.method === "GET" && path === "/status") {
      try {
        const estado = await lerEstado(env);
        const salvoEm = Number(estado.salvoEm || 0);
        const idadeMs = salvoEm ? Date.now() - salvoEm : null;
        const fresco = salvoEm > 0 && idadeMs < STATUS_VELHO_MS;
        return jsonResp({
          ok: fresco,
          versao: VERSAO,
          salvoEm: salvoEm ? new Date(salvoEm).toISOString() : null,
          idadeSeg: idadeMs === null ? null : Math.round(idadeMs / 1000),
          pendentes: Array.isArray(estado.pendentes) ? estado.pendentes.length : 0,
          dryRun: dryRun(env),
        }, fresco ? 200 : 503);
      } catch (e) {
        return jsonResp({ ok: false, versao: VERSAO, erro: truncar(limpar(env, e), 200) }, 500);
      }
    }

    if (req.method === "GET" && path === "/config") {
      if (!autorizado(req, env, null)) return jsonResp({ ok: false, erro: "não autorizado" }, 401);
      try {
        const cfg = await lerConfig(env);
        return jsonResp({ ok: true, carteiras: cfg.carteiras, atualizado: cfg.atualizado, versao: VERSAO });
      } catch (e) {
        return jsonResp({ ok: false, erro: truncar(limpar(env, e), 300) }, 400);
      }
    }

    if (req.method !== "POST" || (path !== "/config" && path !== "/notify")) {
      return textResp("não encontrado", 404);
    }

    let body = null;
    try { body = await req.json(); } catch (_) { return jsonResp({ ok: false, erro: "corpo JSON inválido" }, 400); }
    if (!autorizado(req, env, body)) return jsonResp({ ok: false, erro: "não autorizado" }, 401);

    try {
      if (path === "/config") {
        const salvos = await salvarCarteirasPayload(env, body);
        await atualizarListaAposConfig(env);
        return jsonResp({ ok: true, ...salvos, versao: VERSAO });
      }

      if (body && (body.titulo || body.corpo)) {
        const topicoId = await garantirTopicoAtualizacoes(env);
        const msg = {
          chat_id: env.TELEGRAM_CHAT_ID,
          text: `<b>${esc(truncar(body.titulo || "Aviso", 200))}</b>\n${esc(truncar(body.corpo || "", 3000))}`,
          parse_mode: "HTML",
          disable_web_page_preview: true,
        };
        if (topicoId) msg.message_thread_id = topicoId;
        const r = await enviarComRetentativa(env, "sendMessage", msg);
        if (!r.ok) return jsonResp({ ok: false, erro: truncar(limpar(env, r.desc || r.status), 300), versao: VERSAO }, 502);
        return jsonResp({ ok: true, enviado: true, dryRun: dryRun(env), versao: VERSAO });
      }

      const out = { ok: true, versao: VERSAO };
      if (body && Array.isArray(body.carteiras)) {
        Object.assign(out, await salvarCarteirasPayload(env, body));
        await atualizarListaAposConfig(env);
      }
      out.ciclo = await ciclo(env, "manual");
      return jsonResp(out);
    } catch (e) {
      if (e && e.conflito) {
        const atual = await lerConfig(env).catch(() => ({ carteiras: [], atualizado: null }));
        return jsonResp({
          ok: false, conflito: true, erro: "lista alterada em outro aparelho",
          carteiras: atual.carteiras, atualizado: atual.atualizado, versao: VERSAO,
        }, 409);
      }
      return jsonResp({ ok: false, erro: truncar(limpar(env, e), 300), versao: VERSAO }, 400);
    }
  },

  async scheduled(_event, env) {
    await ciclo(env, "cron");
  },
};
