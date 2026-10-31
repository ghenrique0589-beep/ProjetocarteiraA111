// Polymarket -> Telegram (e-sports)
// Module Worker (Cloudflare Workers)
// Cron recomendado: * * * * *
//
// Secrets (Worker Settings > Variables and Secrets):
//   CHAVE                  token privado para proteger POST /config e POST /notify
//   TELEGRAM_BOT_TOKEN     token do bot; o bot deve ser administrador com gestão de tópicos
//   TELEGRAM_CHAT_ID       ID do supergrupo com tópicos ativados
//
// Binding KV (namespace binding name: CFG):
//   CFG / CFG                  configuração das carteiras
//   CFG / estado4              fila, cursores, deduplicação, tópicos, resumo do dia, eventos
//   CFG / topico_atualizacoes  ID do tópico "Atualizações"
//   CFG / topicos              legado: lido só na migração, não é mais gravado
//
// Gravações no KV (plano grátis: 1.000 por dia, renovam à meia-noite UTC):
//   - o estado só é gravado quando algo muda de verdade, ou a cada 15 min (checkpoint);
//   - ANTES de enviar, grava o que foi capturado. Se essa gravação falhar, nada é enviado,
//     então cota esgotada não vira spam a cada minuto;
//   - DEPOIS de enviar, grava de novo para tirar da fila o que saiu.
// Sem trava no KV: cada ciclo tem prazo (PRAZO_*) para não se sobrepor ao seguinte.

const DATA_API = "https://data-api.polymarket.com";
const GAMMA_API = "https://gamma-api.polymarket.com";
const FUSO = "America/Sao_Paulo";

const KV_CFG = "CFG";
const KV_ESTADO = "estado4";
const KV_ESTADO_ANTIGO = "estado3";
const KV_TOPICOS_LEGADO = "topicos";

const VERSAO = "2026-10-30-kv-v3";
const NOME_TOPICO_ATUALIZACOES = "Atualizações";

const ESPORT = /(?:^|[-_\s/])(?:league[-_\s]?of[-_\s]?legends|lol|counter[-_\s]?strike|cs2|csgo|cs-go|dota[-_\s]?2?|valorant|rainbow[-_\s]?six|r6siege|r6|call[-_\s]?of[-_\s]?duty|cod(?:mw\d?)?|overwatch|ow2|rocket[-_\s]?league|rlcs|starcraft|mobile[-_\s]?legends|mlbb|honor[-_\s]?kings|hok|king[-_\s]?glory|kog|wild[-_\s]?rift|arena[-_\s]?of[-_\s]?valor)(?:$|[-_\s/])/i;

const CARTEIRAS_POR_CICLO = 20;
const PAGINAS_POR_CARTEIRA_CICLO = 1;
const LIMITE_PAGINA = 500;
const CONCORRENCIA = 4;
const SOBREPOSICAO_S = 300;
const JANELA_INICIAL_S = 3600;
const JANELA_MAXIMA_S = 3 * 3600; // após uma falha longa, não recupera trades mais antigos que isso
const MAX_PENDENTES = 3000;
const MAX_AVISOS = 6;
const MAX_TENTATIVAS = 10; // teto do contador de falhas: falha persistente não gera gravação infinita
const DIAS_TOPICO = 30;
const TIMEOUT_FETCH_MS = 10000;
const TENTATIVAS_METADADOS = 3;
const RETRY_METADADOS_S = 30;
const CHECKPOINT_MS = 15 * 60 * 1000;
const PRAZO_COLETA_MS = 25000;
const PRAZO_ENVIO_MS = 45000;

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
function chaveTrade(n) {
  return [
    n.tx || "sem-tx", n.asset || "sem-asset", n.cond, n.idx, n.side, n.ts,
    n.size.toFixed(6), n.price.toFixed(6), chaveTexto(n.outcome),
  ].join("|");
}
function chaveTradeAntiga(n) {
  return [n.tx, n.cond, n.idx, n.side, n.size.toFixed(4), n.price.toFixed(4)].join("|");
}
// Timestamp (s) embutido na chave: campo 6 de 9 (ver chaveTrade). NaN para chaves antigas.
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
  if (/\b(total\s+)?kirst\s+(?:tower|dragon|objective)\b|\btotal\s+rounds?\b|\brounds?\s+(?:over|under)\b|\btotal\s+points?\b/i.test(limpo)) {
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

async function enviarComRetentativa(env, metodo, corpo, tentativas = 3) {
  let ultima;
  for (let i = 0; i < tentativas; i++) {
    try {
      const r = await fetchComTimeout(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${metodo}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(corpo),
      });
      const j = await r.json().catch(() => ({}));
      if (j.ok) return { ok: true, result: j.result, status: r.status };
      ultima = { ok: false, status: r.status, desc: j.description || `HTTP ${r.status}` };
      if (r.status === 429) {
        const retry = Number(j.parameters && j.parameters.retry_after) || 5;
        await pausa(retry * 1000);
        continue;
      }
      if (r.status >= 500 && i < tentativas - 1) {
        await pausa(400 * (i + 1));
        continue;
      }
      return ultima;
    } catch (e) {
      ultima = { ok: false, status: 0, desc: String(e && e.message || e) };
      if (i < tentativas - 1) await pausa(300 * (i + 1));
    }
  }
  return ultima || { ok: false, status: 0, desc: "falha desconhecida" };
}

function nomeCarteira(cfg, w) {
  const c = cfg.carteiras.find(x => x.w === w);
  return (c && c.nome) || w.slice(0, 8) + "…";
}

async function garantirTopicoAtualizacoes(env) {
  const raw = await env.CFG.get("topico_atualizacoes");
  if (raw) {
    const id = Number(raw);
    if (id > 0) return id;
  }
  const r = await enviarComRetentativa(env, "createForumTopic", {
    chat_id: env.TELEGRAM_CHAT_ID,
    name: NOME_TOPICO_ATUALIZACOES,
  });
  if (!r.ok || !r.result || !r.result.message_thread_id) {
    console.warn("falha ao criar tópico Atualizações: " + limpar(env, r.desc || r.status));
    return 0;
  }
  const id = r.result.message_thread_id;
  await env.CFG.put("topico_atualizacoes", String(id));
  return id;
}

function textoListaCarteiras(cfg) {
  if (!cfg.carteiras.length) {
    return `📋 <b>CARTEIRAS MONITORADAS</b> · 0\n\n<i>Nenhuma carteira no momento.</i>\n\n<code>${VERSAO}</code>`;
  }
  const linhas = cfg.carteiras.map((c, i) => {
    const nome = c.nome ? esc(c.nome) : "<i>sem nome</i>";
    return `<b>${String(i + 1).padStart(2, "0")}.</b> ${nome}\n<code>${c.w}</code>`;
  });
  return `📋 <b>CARTEIRAS MONITORADAS</b> · ${cfg.carteiras.length}\n\n` + linhas.join("\n\n") + `\n\n<code>${VERSAO}</code> · atualizado ${horaSegBR(Date.now())}`;
}

async function atualizarListaCarteiras(env, estado, cfg, forcar = false) {
  const hashAtual = cfg.carteiras.map(c => c.w + "|" + (c.nome || "")).join(",");
  const diaHoje = diaBR(Date.now());
  if (!estado.lista) estado.lista = { msgId: null, hash: "", dia: "", mudou: true };
  const mudouLista = estado.lista.hash !== hashAtual;
  const mudouDia = estado.lista.dia !== diaHoje;
  if (!forcar && !mudouLista && !mudouDia && !estado.lista.mudou) return false;
  const topicoId = await garantirTopicoAtualizacoes(env);
  if (!topicoId) return false;
  const texto = textoListaCarteiras(cfg);
  const corpo = {
    chat_id: env.TELEGRAM_CHAT_ID, text: texto, parse_mode: "HTML",
    disable_web_page_preview: true, message_thread_id: topicoId,
  };
  if (estado.lista.msgId) {
    const edit = await enviarComRetentativa(env, "editMessageText", { ...corpo, message_id: estado.lista.msgId });
    if (edit.ok || /not[ _]modified/i.test(edit.desc || "")) {
      estado.lista.hash = hashAtual; estado.lista.dia = diaHoje; estado.lista.mudou = false; return true;
    }
    if (/message to edit not found|message_id_invalid|can't be edited/i.test(edit.desc || "")) {
      estado.lista.msgId = null;
    } else {
      console.warn("lista carteiras (edit): " + limpar(env, edit.desc).slice(0, 160));
      return false;
    }
  }
  const novo = await enviarComRetentativa(env, "sendMessage", corpo);
  if (novo.ok && novo.result) {
    estado.lista.msgId = novo.result.message_id;
    estado.lista.hash = hashAtual; estado.lista.dia = diaHoje; estado.lista.mudou = false; return true;
  }
  console.warn("lista carteiras (send): " + limpar(env, novo.desc).slice(0, 160));
  return false;
}

function registrarAposta(estado, g) {
  const dia = diaBR(Date.now());
  if (!estado.resumo || estado.resumo.dia !== dia) {
    estado.resumo = { dia, apostas: {}, nomes: {}, msgId: null, mudou: true };
  }
  const r = estado.resumo;
  for (const it of g.itens) { r.apostas[it.w] = (r.apostas[it.w] || 0) + 1; }
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
  if (!novo.ok && topicoResumo && novo.status === 400 && /thread|topic/i.test(novo.desc || "")) novo = await enviarComRetentativa(env, "sendMessage", corpo);
  if (novo.ok && novo.result) { r.msgId = novo.result.message_id; r.mudou = false; return true; }
  console.warn("resumo diário não atualizado: " + limpar(env, novo.desc).slice(0, 160));
  return false;
}

async function lerConfig(env) {
  const raw = await env.CFG.get(KV_CFG);
  let v = {};
  try { v = raw ? JSON.parse(raw) : {}; } catch (_) { throw new Error("configuração CFG corrompida"); }
  const lista = Array.isArray(v.carteiras) ? v.carteiras : [];
  const carteiras = [];
  const vistos = new Set();
  for (const c of lista) {
    const w = String(c && c.w || "").trim().toLowerCase();
    if (!/^0x[a-f0-9]{40}$/.test(w) || vistos.has(w)) continue;
    vistos.add(w);
    carteiras.push({ w, nome: String(c.nome || "").trim().slice(0, 40) });
  }
  return { carteiras, atualizado: typeof v.atualizado === "string" ? v.atualizado : null };
}
async function versaoAtual(env) {
  const raw = await env.CFG.get(KV_CFG);
  if (!raw) return null;
  try { const v = JSON.parse(raw); return typeof v.atualizado === "string" ? v.atualizado : null; } catch (_) { return null; }
}
async function salvarCarteiras(env, carteiras, base) {
  if (!Array.isArray(carteiras)) { throw new Error('payload inválido: esperado { "carteiras": [{ "w": "0x...", "nome": "..." }] }'); }
  const vistos = new Set();
  const lista = [];
  let ignoradas = 0;
  for (const c of carteiras) {
    const w = String(c && c.w || "").trim().toLowerCase();
    if (!/^0x[a-f0-9]{40}$/.test(w) || vistos.has(w)) { ignoradas++; continue; }
    vistos.add(w);
    lista.push({ w, nome: String(c && c.nome || "").trim().slice(0, 40) });
  }
  if (base !== undefined && base !== await versaoAtual(env)) {
    const erro = new Error("lista alterada em outro aparelho");
    erro.conflito = true;
    throw erro;
  }
  const atualizado = new Date().toISOString();
  await env.CFG.put(KV_CFG, JSON.stringify({ carteiras: lista, atualizado }));
  return { salvas: lista.length, ignoradas, atualizado };
}
async function migrarFilaAntiga(env) {
  try {
    const raw = await env.CFG.get(KV_ESTADO_ANTIGO);
    if (!raw) return [];
    const a = JSON.parse(raw);
    const lista = [a.pendentes, a.pending, a.fila, a.queue].find(Array.isArray) || [];
    const out = [];
    for (const item of lista) {
      const n = normalizar((item && (item.t || item.trade)) || item);
      if (!n || !n.cond || !eEsports(n)) continue;
      const w = String((item && item.c && item.c.w) || (item && item.w) || n.w).toLowerCase();
      if (!/^0x[a-f0-9]{40}$/.test(w)) continue;
      const id = `${w}|${chaveTrade(n)}`;
      out.push({ id, w, n, tent: 0, nextTryAt: 0 });
    }
    return out;
  } catch (e) {
    console.warn("migração de fila antiga: " + limpar(env, e).slice(0, 120));
    return [];
  }
}

// Leitura única da chave legada "topicos" (só para migração).
async function lerTopicosLegado(env) {
  const raw = await env.CFG.get(KV_TOPICOS_LEGADO);
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
  const raw = await env.CFG.get(KV_ESTADO);
  let e;
  if (raw) {
    try { e = JSON.parse(raw); } catch (_) { throw new Error("estado4 corrompido; interrompi para não sobrescrever a fila"); }
  } else {
    e = { pendentes: await migrarFilaAntiga(env), vistos: {}, ultimo: {}, coletas: {}, resumo: null, lista: null, eventos: {} };
  }
  delete e.indiceCarteira; // campo da versão anterior; o rodízio agora usa o relógio
  e.pendentes = Array.isArray(e.pendentes) ? e.pendentes.filter(it => it && it.n && it.w) : [];
  e.vistos = e.vistos && typeof e.vistos === "object" ? e.vistos : {};
  e.ultimo = e.ultimo && typeof e.ultimo === "object" ? e.ultimo : {};
  e.coletas = e.coletas && typeof e.coletas === "object" ? e.coletas : {};
  e.resumo = e.resumo && typeof e.resumo === "object" ? e.resumo : null;
  e.lista = e.lista && typeof e.lista === "object" ? e.lista : null;
  e.eventos = e.eventos && typeof e.eventos === "object" && !Array.isArray(e.eventos) ? e.eventos : {};
  return e;
}

// Tópicos vinham da chave "topicos"; agora ficam dentro do estado4 (uma gravação só).
async function garantirTopicos(env, estado) {
  if (estado.topicos && typeof estado.topicos === "object" && !Array.isArray(estado.topicos)) return;
  const legado = await lerTopicosLegado(env);
  if (legado === null) throw new Error("registro de tópicos ilegível; ciclo interrompido para evitar duplicações");
  estado.topicos = legado;
}

// Fotografia do estado para saber se algo mudou de verdade. Ficam de fora: ultimo e salvoEm
// (mudam a cada ciclo) e nextTryAt (só controla o intervalo de reenvio).
function semCheckpoint(e) {
  const { ultimo, salvoEm, pendentes, ...resto } = e;
  return JSON.stringify(resto) + "|" + JSON.stringify((pendentes || []).map(it => [it.id, it.tent]));
}
function precisaGravar(estado, base) {
  return semCheckpoint(estado) !== base || Date.now() - Number(estado.salvoEm || 0) > CHECKPOINT_MS;
}

// Até onde a deduplicação ainda precisa olhar: trades anteriores a esse ponto nunca voltam numa busca futura.
function corteVistos(estado, w) {
  if (estado.ultimo[w]) return Number(estado.ultimo[w]) - SOBREPOSICAO_S;
  const job = estado.coletas[w];
  return job && job.start ? Number(job.start) : 0;
}

// Poda antes de cada gravação: vistos antigos saem (exceto os ainda pendentes), tópicos com mais de
// DIAS_TOPICO dias saem, e vínculos de evento sem tópico também.
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
  await env.CFG.put(KV_ESTADO, JSON.stringify(estado));
}

// Rodízio pelo relógio (não precisa gravar nada): cada minuto desloca a janela de 20 carteiras.
function selecionarCarteiras(carteiras) {
  if (carteiras.length <= CARTEIRAS_POR_CICLO) return carteiras;
  const inicio = (Math.floor(Date.now() / 60000) * CARTEIRAS_POR_CICLO) % carteiras.length;
  return Array.from({ length: CARTEIRAS_POR_CICLO }, (_, i) => carteiras[(inicio + i) % carteiras.length]);
}

async function coletarCarteira(c, estado, pendPorW) {
  const w = c.w;
  const agora = agoraS();
  const existentes = new Set(Array.isArray(estado.vistos[w]) ? estado.vistos[w] : []);
  for (const k of pendPorW.get(w) || []) existentes.add(k);

  let job = estado.coletas[w];
  if (!job || typeof job !== "object") {
    const checkpoint = Number(estado.ultimo[w] || 0);
    const inicio = checkpoint
      ? Math.max(checkpoint - SOBREPOSICAO_S, agora - JANELA_MAXIMA_S)
      : agora - JANELA_INICIAL_S;
    job = { start: Math.max(1, inicio), end: agora, cursor: null };
    estado.coletas[w] = job;
  }
  let novos = 0;
  let paginas = 0;
  let concluido = false;
  try {
    while (paginas < PAGINAS_POR_CARTEIRA_CICLO) {
      const cursorAntes = job.cursor || null;
      const r = await fetchDados(montarUrlTrades(w, job.start, job.end, cursorAntes), { headers: { Accept: "application/json" } });
      if (!r.ok) throw new Error(`Data API HTTP ${r.status}${r.status === 429 ? " (rate limited; cursor preservado)" : ""}`);
      const body = await r.json();
      const trades = listaDe(body);
      let filaCheia = false;
      for (const bruto of trades) {
        const n = normalizar(bruto);
        if (!n || !n.cond || !eEsports(n)) continue;
        const k = chaveTrade(n);
        if (existentes.has(k)) continue;
        if (existentes.has(chaveTradeAntiga(n))) { existentes.add(k); continue; }
        if (estado.pendentes.length >= MAX_PENDENTES) { filaCheia = true; break; }
        existentes.add(k);
        estado.pendentes.push({ id: `${w}|${k}`, w, n, tent: 0, nextTryAt: 0 });
        novos++;
      }
      paginas++;
      const next = body && body.pagination ? body.pagination.next_cursor : null;
      if (filaCheia) { job.cursor = cursorAntes; break; }
      if (next) {
        job.cursor = String(next);
        if (paginas >= PAGINAS_POR_CARTEIRA_CICLO) break;
      } else {
        concluido = true;
        delete estado.coletas[w];
        estado.ultimo[w] = job.end;
        break;
      }
    }
  } finally {
    if (existentes.size) estado.vistos[w] = [...existentes];
    else delete estado.vistos[w];
    if (!concluido) estado.coletas[w] = job;
  }
  return { novos, paginas };
}

async function coletar(cfg, estado, prazo) {
  const alvos = selecionarCarteiras(cfg.carteiras);
  const pendPorW = new Map();
  for (const it of estado.pendentes) {
    if (!pendPorW.has(it.w)) pendPorW.set(it.w, []);
    pendPorW.get(it.w).push(it.id.slice(it.w.length + 1));
  }
  let novos = 0, paginas = 0, erros = 0, puladas = 0;
  await mapLimit(alvos, CONCORRENCIA, async c => {
    if (prazo()) { puladas++; return; }
    try {
      const r = await coletarCarteira(c, estado, pendPorW);
      novos += r.novos;
      paginas += r.paginas;
    } catch (e) {
      erros++;
      console.warn(`[${c.w.slice(0, 8)}] ${String(e && e.message || e).slice(0, 180)}`);
    }
  });
  return { novos, paginas, erros, puladas, consultadas: alvos.length - puladas };
}

// Carteira removida: limpa tudo dela (inclusive o checkpoint, para não gerar enxurrada ao readicionar).
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
  for (const w of Object.keys(r.apostas)) { if (!cfg.carteiras.some(c => c.w === w)) { delete r.apostas[w]; mudou = true; } }
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
    } catch (e) { console.warn("buscarEvento: " + limpar(env, e).slice(0, 120)); }
    return ev;
  };
  let ev = await tentar();
  if (!ev) { await pausa(400); ev = await tentar(); }
  cacheEventos.set(chaveCache, ev);
  return ev;
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

function textoGrupo(env, g, cfg, ev) {
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
    const chaveM = `${d.classificacao.rotulo.replace(' da partida', '')} - ${d.escolha}`;
    const valor = Number(it.n.size || 0) * Number(it.n.price || 0);
    if (!topico.resumo.mercados[chaveM]) {
      topico.resumo.mercados[chaveM] = { valor: 0, qtd: 0 };
    }
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
    `💰 <b>TOTAL GERAL:</b> ${moeda(totalGeral)} · ${totalApostas} aposta(s)`;
  if (topico.resumo.msgId) {
    const edit = await enviarComRetentativa(env, "editMessageText", {
      chat_id: env.TELEGRAM_CHAT_ID, message_id: topico.resumo.msgId, text: texto,
      parse_mode: "HTML", disable_web_page_preview: true
    });
    if (edit.ok || /message is not modified|not[ _]modified/i.test(edit.desc || "")) return;
    if (/message to edit not found|message_id_invalid|can't be edited/i.test((edit.desc || "").toLowerCase())) topico.resumo.msgId = null;
    else { console.warn("Falha ao editar placar: " + limpar(env, edit.desc).slice(0, 160)); return; }
  }
  if (!topico.resumo.msgId) {
    const novo = await enviarComRetentativa(env, "sendMessage", {
      chat_id: env.TELEGRAM_CHAT_ID, message_thread_id: topico.id, text: texto,
      parse_mode: "HTML", disable_web_page_preview: true
    });
    if (novo.ok && novo.result) topico.resumo.msgId = novo.result.message_id;
    else console.warn("Falha ao enviar placar: " + limpar(env, novo.desc || novo.status).slice(0, 160));
  }
}

function novoTopico(id, nome, ev) {
  return {
    id, nome,
    inicioMs: (ev && ev.inicioMs) || 0,
    dia: (ev && ev.data) || diaBR(Date.now()),
    resumo: { msgId: null, mercados: {} },
  };
}

async function enviarGrupo(env, g, cfg, estado, cacheEventos) {
  if (!g || !g.itens || !g.itens.length) return { ok: false, desc: "grupo vazio" };
  const topicos = estado.topicos;
  const n = g.itens[0].n;
  const ev = await buscarEvento(env, n, cacheEventos);
  const tent = Math.max(...g.itens.map(it => Number(it.tent || 0)));

  if (!ev && tent < TENTATIVAS_METADADOS) {
    return { ok: false, status: 0, desc: `aguardando metadados [${tent + 1}/${TENTATIVAS_METADADOS}]`, retryAfter: RETRY_METADADOS_S };
  }

  let chaveEv = null;
  const persistido = estado.eventos[n.cond];
  if (persistido && topicos[persistido]) chaveEv = persistido;
  if (!chaveEv) chaveEv = (ev && ev.slug) || n.eventSlug || n.cond;

  let topico = topicos[chaveEv];
  if (!topico || !(topico.id > 0)) {
    const nome = nomeTopicoPartida(ev, n);
    const r = await enviarComRetentativa(env, "createForumTopic", { chat_id: env.TELEGRAM_CHAT_ID, name: nome });
    if (!r.ok || !r.result || !r.result.message_thread_id) {
      return { ok: false, status: r.status || 0, desc: "falha ao criar tópico: " + (r.desc || "") };
    }
    topico = novoTopico(r.result.message_thread_id, nome, ev);
    topicos[chaveEv] = topico;
  }
  estado.eventos[n.cond] = chaveEv;

  const corpo = { chat_id: env.TELEGRAM_CHAT_ID, message_thread_id: topico.id, text: textoGrupo(env, g, cfg, ev), parse_mode: "HTML", disable_web_page_preview: true };
  let r = await enviarComRetentativa(env, "sendMessage", corpo);

  if (!r.ok && r.status === 400 && /thread|topic|TOPIC_ID_INVALID|TOPIC_DELETED/i.test(r.desc || "")) {
    delete topicos[chaveEv];
    const nome = nomeTopicoPartida(ev, n);
    const rec = await enviarComRetentativa(env, "createForumTopic", { chat_id: env.TELEGRAM_CHAT_ID, name: nome });
    if (rec.ok && rec.result && rec.result.message_thread_id) {
      topico = novoTopico(rec.result.message_thread_id, nome, ev);
      topicos[chaveEv] = topico;
      r = await enviarComRetentativa(env, "sendMessage", { ...corpo, message_thread_id: topico.id });
    }
  }

  if (r.ok && r.result) {
    registrarNoResumo(topico, g);
    await atualizarPlacarVivo(env, topico);
    return { ok: true, result: r.result, topicoId: topico.id };
  }
  return { ok: false, status: r.status || 0, desc: r.desc || `HTTP ${r.status}`, retryAfter: r.status === 429 ? 5 : 0 };
}

async function ciclo(env, origem) {
  const inicio = Date.now();
  const prazoColeta = () => Date.now() - inicio > PRAZO_COLETA_MS;
  const prazoEnvio = () => Date.now() - inicio > PRAZO_ENVIO_MS;
  const resultado = { origem, carteiras: 0, consultadas: 0, novos: 0, enviados: 0, falhas: 0, pendentes: 0, erros: [] };
  const faltam = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "CFG"].filter(k => !env[k]);
  if (faltam.length) return { ...resultado, erros: ["faltam configurações: " + faltam.join(", ")] };

  try {
    const cfg = await lerConfig(env);
    resultado.carteiras = cfg.carteiras.length;
    const estado = await lerEstado(env);
    const base = semCheckpoint(estado); // fotografia antes de qualquer mudança deste ciclo
    await garantirTopicos(env, estado);
    podarCarteirasRemovidas(estado, cfg);
    sincronizarResumo(estado, cfg);
    await atualizarListaCarteiras(env, estado, cfg);

    if (estado.pendentes.length < MAX_PENDENTES && cfg.carteiras.length) {
      const coleta = await coletar(cfg, estado, prazoColeta);
      resultado.novos = coleta.novos;
      resultado.consultadas = coleta.consultadas;
      if (coleta.erros) resultado.erros.push(`${coleta.erros} carteira(s) não puderam ser consultadas neste ciclo`);
    }

    // 1ª gravação: o que foi capturado, ANTES de enviar. Se falhar, nada é enviado.
    if (precisaGravar(estado, base)) await gravar(env, estado);

    const cacheEventos = new Map();
    const enviados = new Set();
    let mudouEnvio = false;
    for (const g of agrupar(estado.pendentes).slice(0, MAX_AVISOS)) {
      if (prazoEnvio()) break; // o restante fica na fila para o próximo ciclo
      mudouEnvio = true;
      try {
        const r = await enviarGrupo(env, g, cfg, estado, cacheEventos);
        if (r.ok) {
          for (const it of g.itens) enviados.add(it.id);
          registrarAposta(estado, g);
          resultado.enviados++;
        } else {
          resultado.falhas++;
          const tentativas = Math.max(...g.itens.map(it => Number(it.tent || 0))) + 1;
          const atraso = Math.max(r.retryAfter || 0, Math.min(300, 5 * (2 ** Math.min(tentativas, 6))));
          for (const it of g.itens) {
            it.tent = Math.min(MAX_TENTATIVAS, (it.tent || 0) + 1);
            it.nextTryAt = Date.now() + atraso * 1000;
          }
          resultado.erros.push(truncar(r.desc || `falha no Telegram (${r.status})`, 220));
        }
      } catch (e) {
        resultado.falhas++;
        for (const it of g.itens) {
          it.tent = Math.min(MAX_TENTATIVAS, (it.tent || 0) + 1);
          it.nextTryAt = Date.now() + 60000;
        }
        resultado.erros.push(truncar(limpar(env, e), 220));
      }
    }

    if (enviados.size) {
      estado.pendentes = estado.pendentes.filter(it => !enviados.has(it.id));
      mudouEnvio = true;
    }
    if (estado.resumo && estado.resumo.mudou) {
      try {
        if (await atualizarResumo(env, estado, cfg)) mudouEnvio = true;
      } catch (e) {
        resultado.erros.push(truncar(limpar(env, e), 200));
      }
    }
    resultado.pendentes = estado.pendentes.length;
    resultado.proximoCursor = Object.keys(estado.coletas).length;

    // 2ª gravação: tira da fila o que saiu e guarda tópicos, resumo e tentativas novos.
    if (mudouEnvio) await gravar(env, estado);
  } catch (e) {
    resultado.erros.push(truncar(limpar(env, e), 300));
  }
  return resultado;
}

async function salvarCarteirasPayload(env, body) { return salvarCarteiras(env, body && body.carteiras, body && body.base); }

// Após mudar a lista: atualiza a mensagem no Telegram e só grava se o estado mudou.
async function atualizarListaAposConfig(env) {
  try {
    const estado = await lerEstado(env);
    const base = semCheckpoint(estado);
    await garantirTopicos(env, estado);
    const cfg = await lerConfig(env);
    await atualizarListaCarteiras(env, estado, cfg, true);
    if (semCheckpoint(estado) !== base) await gravar(env, estado);
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
      return textResp(`ok · ${VERSAO}`);
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
        const msg = { chat_id: env.TELEGRAM_CHAT_ID, text: `<b>${esc(truncar(body.titulo || "Aviso", 200))}</b>\n${esc(truncar(body.corpo || "", 3000))}`, parse_mode: "HTML", disable_web_page_preview: true };
        if (topicoId) msg.message_thread_id = topicoId;
        const r = await enviarComRetentativa(env, "sendMessage", msg);
        if (!r.ok) return jsonResp({ ok: false, erro: truncar(limpar(env, r.desc || r.status), 300), versao: VERSAO }, 502);
        return jsonResp({ ok: true, enviado: true, versao: VERSAO });
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
        return jsonResp({ ok: false, conflito: true, erro: "lista alterada em outro aparelho", carteiras: atual.carteiras, atualizado: atual.atualizado, versao: VERSAO }, 409);
      }
      return jsonResp({ ok: false, erro: truncar(limpar(env, e), 300), versao: VERSAO }, 400);
    }
  },

  async scheduled(_event, env) {
    await ciclo(env, "cron");
  },
};
