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
//   CFG / CFG               configuração das carteiras
//   CFG / estado4           fila, cursores, deduplicação e resumo do dia
//   CFG / topicos           tópicos por partida
//   CFG / lock              trava de execução (best effort)
//
// GET /config: (Authorization: Bearer CHAVE) devolve { carteiras, atualizado }
// POST /config: { "carteiras": [{ "w": "0x...", "nome": "Minha carteira" }], "base": "<atualizado lido antes>" }
//   base é opcional. Se a lista tiver mudado em outro aparelho desde essa leitura,
//   responde 409 com a lista atual, sem sobrescrever nada.
// POST /notify: payload opcional igual ao /config; salva e executa um ciclo agora
// GET /: health check básico
//
// O processamento usa paginação persistente: se houver mais páginas, o cursor é
// retomado no próximo ciclo. Falhas de envio não apagam apostas da fila.

const DATA_API = "https://data-api.polymarket.com";
const GAMMA_API = "https://gamma-api.polymarket.com";
const FUSO = "America/Sao_Paulo";

const KV_CFG = "CFG";
const KV_ESTADO = "estado4";
const KV_ESTADO_ANTIGO = "estado3";
const KV_TOPICOS = "topicos";
const KV_LOCK = "lock";

// Recogniza slugs comuns sem confundir siglas curtas como "val" ou "rl" com texto comum.
const ESPORT = /(?:^|[-_\s/])(?:league[-_\s]?of[-_\s]?legends|lol|counter[-_\s]?strike|cs2|csgo|cs-go|dota[-_\s]?2?|valorant|rainbow[-_\s]?six|r6siege|r6|call[-_\s]?of[-_\s]?duty|cod(?:mw\d?)?|overwatch|ow2|rocket[-_\s]?league|rlcs|starcraft|mobile[-_\s]?legends|mlbb|honor[-_\s]?of[-_\s]?kings|hok|king[-_\s]?of[-_\s]?glory|kog|wild[-_\s]?rift|arena[-_\s]?of[-_\s]?valor)(?:$|[-_\s/])/i;

// Limites de trabalho por ciclo: continuam em ciclos seguintes, sem descartar histórico.
const CARTEIRAS_POR_CICLO = 20;
const PAGINAS_POR_CARTEIRA_CICLO = 1;
const LIMITE_PAGINA = 500;
const CONCORRENCIA = 4;
const SOBREPOSICAO_S = 300;
const JANELA_INICIAL_S = 3600;
const MAX_VISTOS = 2000;
const MAX_PENDENTES = 3000;
const MAX_AVISOS = 6;
const LOCK_MS = 55000;
const DIAS_TOPICO = 30;
const TIMEOUT_FETCH_MS = 10000;

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

// -----------------------------------------------------------------------------
// Utilitários
// -----------------------------------------------------------------------------
const esc = v => String(v ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const truncar = (v, n) => { const s = String(v ?? ""); return s.length > n ? s.slice(0, n - 1) + "…" : s; };
const pausa = ms => new Promise(resolve => setTimeout(resolve, ms));
const agoraS = () => Math.floor(Date.now() / 1000);
const chaveTexto = v => String(v ?? "").normalize("NFD").replace(/[\u0300-\u036f]/g, "")
  .toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
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

const fmtHora = new Intl.DateTimeFormat("pt-BR", {
  timeZone: FUSO, hour: "2-digit", minute: "2-digit", hourCycle: "h23",
});
const fmtHoraSeg = new Intl.DateTimeFormat("pt-BR", {
  timeZone: FUSO, hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
});
const fmtDiaPartes = new Intl.DateTimeFormat("en-CA", {
  timeZone: FUSO, year: "numeric", month: "2-digit", day: "2-digit",
});
const horaBR = ms => fmtHora.format(new Date(ms));
const horaSegBR = ms => fmtHoraSeg.format(new Date(ms));
function diaBR(ms) {
  const p = Object.fromEntries(fmtDiaPartes.formatToParts(new Date(ms)).map(x => [x.type, x.value]));
  return `${p.year}-${p.month}-${p.day}`;
}
const diaCurtoBR = ms => `${diaBR(ms).slice(8, 10)}/${diaBR(ms).slice(5, 7)}`;
const diaComAnoBR = iso => {
  const [y, m, d] = String(iso || "").split("-");
  return y && m && d ? `${d}/${m}/${y}` : "data não identificada";
};

// Aceita segundos, milissegundos, datas ISO e "2026-10-23 19:30:00+00".
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
  // Uma data sem horário não deve ser exibida falsamente como início à meia-noite.
  if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}$/.test(v.trim())) return 0;
  return paraMs(v);
}

// -----------------------------------------------------------------------------
// Data API: normalização, filtro e identidade de cada execução
// -----------------------------------------------------------------------------
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
  // A API não fornece um ID de fill universal; a combinação abaixo reduz colisões
  // entre fills na mesma transação e também funciona quando o hash está ausente.
  return [
    n.tx || "sem-tx", n.asset || "sem-asset", n.cond, n.idx, n.side, n.ts,
    n.size.toFixed(6), n.price.toFixed(6), chaveTexto(n.outcome),
  ].join("|");
}
function chaveTradeAntiga(n) {
  // Formato usado na versão anterior: permite migrar a deduplicação sem reenviar apostas recentes.
  return [n.tx, n.cond, n.idx, n.side, n.size.toFixed(4), n.price.toFixed(4)].join("|");
}
function eEsports(n) {
  const slugTexto = `${n.eventSlug || ""} ${n.slug || ""}`;
  if (ESPORT.test(slugTexto)) return true;
  // Alguns slugs de mercados omitem o nome do jogo; use sinais específicos de e-sports.
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

// -----------------------------------------------------------------------------
// Rede e retentativas
// -----------------------------------------------------------------------------
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
      // Não repetir 429 de imediato; o cursor fica salvo para a próxima execução.
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

// -----------------------------------------------------------------------------
// Times e classificação de mercados
// -----------------------------------------------------------------------------
const NAO_TIME = /^(yes|no|over|under|sim|nao|map|mapa|game|total|winner|handicap)$/i;
const JOGO = /^(counter strike.*|cs2?|csgo|dota.*|league of legends|lol|valorant|rocket league|rainbow six.*|r6.*|call of duty|cod|overwatch.*|starcraft.*|mobile legends|mlbb|esports?)$/i;
const limparTime = v => String(v || "").replace(/\([^)]*\)/g, " ").replace(/\s+/g, " ")
  .replace(/^[\s:|–—-]+|[\s:|–—-]+$/g, "").trim();

// Ex.: "Counter-Strike: NAVI vs FaZe (BO3) - Map 1 Winner" -> NAVI / FaZe.
function extrairTimes(texto) {
  const s = String(texto || "").replace(/\s+/g, " ").trim();
  const m = s.match(/^(.+?)\s+(?:vs\.?|versus)\s+(.+)$/i);
  if (!m) return null;
  const antesDoVs = m[1].replace(/^.*:\s*/, "");
  const depoisDoVs = m[2].split(/\s+[-–—|]\s+/)[0]
    .split(/\s+(?=(?:map|mapa|game|jogo|winner|total|handicap|over|under|moneyline|bo\s*\d)\b)/i)[0];
  const a = limparTime(antesDoVs);
  const b = limparTime(depoisDoVs);
  if (!a || !b || a.length > 50 || b.length > 50) return null;
  if (NAO_TIME.test(chaveTexto(a)) || NAO_TIME.test(chaveTexto(b))) return null;
  if (JOGO.test(chaveTexto(a)) || JOGO.test(chaveTexto(b))) return null;
  return { a, b };
}

const alvoMercado = n => `${n.titulo || ""} ${String(n.slug || "").replace(/[-_]+/g, " ")}`
  .replace(/\s+/g, " ").trim();
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
  if (/\b(total\s+)?kills?\b|\bfirst\s+(?:tower|dragon|objective)\b|\btotal\s+rounds?\b|\brounds?\s+(?:over|under)\b|\btotal\s+points?\b/i.test(limpo)) {
    let rotulo = /kills?/i.test(limpo) ? "Kills" : /rounds?/i.test(limpo) ? "Total de rodadas" : /tower|dragon|objective/i.test(limpo) ? "Objetivo da partida" : "Outro mercado";
    if (numMapa) rotulo += ` · mapa ${numMapa}`;
    return { tipo: "outro", rotulo };
  }
  if (numMapa) return { tipo: "mapa", rotulo: `Mapa ${numMapa}` };

  const temTimes = !!extrairTimes(n.titulo);
  if (temTimes || /\b(match\s+winner|winner\s+of\s+(?:the\s+)?match|match\s+result|moneyline|series\s+winner|who\s+will\s+win|win\s+(?:the\s+)?match|vencedor\s+da\s+partida)\b/i.test(limpo)) {
    return { tipo: "vencedor", rotulo: "Vencedor da partida" };
  }
  // Não chamar mercados desconhecidos de "Vencedor": preserva o nome verdadeiro.
  return { tipo: "outro", rotulo: truncar(n.titulo || n.slug || "Mercado não classificado", 65) };
}

function linhaTotal(n) {
  // Remova a data no slug ANTES de substituir hífens; assim 2026-10-23 nunca vira a linha "10".
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
    const m = String(n.titulo || "").match(/:\s*(.+?)\s*\(([+-]?\d+(?:[.,]\d+)?)\)/);
    if (m) {
      const valor = Number(m[2].replace(",", "."));
      const linha = chaveTexto(m[1]) === chaveTexto(o) ? valor : -valor;
      escolha = `${o} (${linha > 0 ? "+" : ""}${String(linha).replace(".", ",")})`;
    }
  }
  return { classificacao: c, escolha, texto: `${c.rotulo} — ${escolha}` };
}

// -----------------------------------------------------------------------------
// Metadados da partida (Gamma): times oficiais, data e horário inicial
// -----------------------------------------------------------------------------
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
  ].map(dataValor).find(Boolean) || null;
  return {
    titulo,
    slug,
    times: extrairTimes(titulo),
    inicioMs,
    dataSlug: dataForte || (inicioMs ? diaBR(inicioMs) : dataPorCampo),
  };
}
async function buscarEvento(env, n, cache) {
  if (!n.eventSlug || n.eventSlug.length > 300) return null;
  const slug = n.eventSlug;
  let ev = cache.get(slug);
  if (ev === undefined) {
    try {
      const r = await fetchDados(`${GAMMA_API}/events/slug/${encodeURIComponent(slug)}`, {
        headers: { Accept: "application/json" },
      });
      if (!r.ok) throw new Error(`Gamma API HTTP ${r.status}`);
      const raw = await r.json();
      ev = Array.isArray(raw) ? raw[0] : raw;
      if (!ev || typeof ev !== "object") ev = null;
    } catch (e) {
      console.warn("Gamma: " + limpar(env, e).slice(0, 140));
      ev = null;
    }
    cache.set(slug, ev);
  }
  return ev ? extrairEvento(ev, n.cond) : null;
}

// -----------------------------------------------------------------------------
// Partidas e tópicos
// -----------------------------------------------------------------------------
const chaveTimes = t => [chaveTexto(t.a), chaveTexto(t.b)].sort().join(" x ");
const chaveDiaTimes = (t, dia) => `${chaveTimes(t)}|${dia}`; // formato usado pelo Worker antigo
function nomeTopico(t, dia, inicioMs) {
  const base = `${t.a} vs ${t.b}`;
  const quando = inicioMs
    ? `${diaCurtoBR(inicioMs)} ${horaBR(inicioMs)}`
    : dia ? `${dia.slice(8, 10)}/${dia.slice(5, 7)} · início a confirmar` : "início a confirmar";
  return truncar(`${base} · ${quando}`, 100);
}
function chavesCompativeis(times, dia, inicioMs, eventSlug) {
  const chaves = [];
  if (times && dia) {
    chaves.push(chaveDiaTimes(times, dia));
    chaves.push(`partida|${chaveTimes(times)}|${dia}|hora?`);
    chaves.push(`partida|${chaveTimes(times)}|${dia}|${inicioMs ? Math.floor(inicioMs / 60000) : "hora?"}`);
  }
  if (eventSlug) chaves.push(`evento|${eventSlug}`);
  return [...new Set(chaves)];
}
async function resolverPartida(env, n, topicos, cache) {
  const diaSlug = dataDoSlug(n.eventSlug) || dataDoSlug(n.slug);
  let times = extrairTimes(n.titulo);
  const chaveEvento = n.eventSlug ? `evento|${n.eventSlug}` : "";
  const regEvento = (chaveEvento && topicos[chaveEvento]) || null;
  const regLegado = (!regEvento && times && diaSlug) ? (topicos[chaveDiaTimes(times, diaSlug)] || null) : null;

  // Se ainda não há registro para o event_slug específico, consulte a Gamma mesmo
  // quando existir um tópico legado com o mesmo par de equipes: pode ser uma revanche.
  const ev = regEvento && regEvento.inicioMs ? null : await buscarEvento(env, n, cache);
  if (!times && ev) times = ev.times;
  const inicioMs = (ev && ev.inicioMs) || (regEvento && regEvento.inicioMs) || (!n.eventSlug && regLegado && regLegado.inicioMs) || 0;
  const dia = (inicioMs ? diaBR(inicioMs) : null) || diaSlug || (ev && ev.dataSlug) || diaBR((n.ts || agoraS()) * 1000);

  if (!times) {
    const key = chaveEvento || `evento|${n.slug || n.cond || "partida-desconhecida"}`;
    const reg = topicos[key] || regEvento || null;
    const tituloEvento = (ev && ev.titulo) || n.eventSlug || n.slug || "Partida sem nome identificado";
    return {
      key, nome: truncar(`${tituloEvento}${inicioMs ? ` · ${diaCurtoBR(inicioMs)} ${horaBR(inicioMs)}` : ` · ${dia.slice(8, 10)}/${dia.slice(5, 7)} · início a confirmar`}`, 100),
      inicioMs, existente: reg, dia, migrarDe: [],
    };
  }

  const chavesAntigas = chavesCompativeis(times, dia, inicioMs, n.eventSlug).filter(k => k !== chaveEvento);
  const key = chaveEvento || `partida|${chaveTimes(times)}|${dia}|${inicioMs ? Math.floor(inicioMs / 60000) : "hora?"}`;
  const compatReg = chavesAntigas.map(k => ({ key: k, reg: topicos[k] })).find(({ reg }) => {
    if (!reg) return false;
    if (reg.dia && reg.dia !== dia) return false;
    // Sem horário confirmado, não mesclar um evento slug novo com tópico antigo cronometrado.
    if (n.eventSlug && !inicioMs && reg.inicioMs) return false;
    // Não reutilizar tópico antigo de uma revanche em horário diferente.
    if (inicioMs && reg.inicioMs && Math.abs(inicioMs - reg.inicioMs) > 5 * 60 * 1000) return false;
    return true;
  }) || null;
  const existente = topicos[key] || compatReg?.reg || regEvento || (!n.eventSlug ? regLegado : null) || null;
  const migrarDe = chavesAntigas.filter(k => topicos[k] && topicos[k] === existente && k !== key);
  return { key, nome: nomeTopico(times, dia, inicioMs), inicioMs, existente, dia, migrarDe };
}

// -----------------------------------------------------------------------------
// Telegram Bot API
// -----------------------------------------------------------------------------
async function tg(env, metodo, corpo) {
  try {
    const r = await fetchComTimeout(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${metodo}`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(corpo),
    });
    let d = null;
    try { d = await r.json(); } catch (_) {}
    return {
      ok: r.ok && !!d && d.ok === true,
      status: r.status,
      desc: d && d.description ? String(d.description).slice(0, 240) : "",
      espera: Number((d && d.parameters && d.parameters.retry_after) || r.headers.get("Retry-After") || 0),
      result: d ? d.result : undefined,
    };
  } catch (e) {
    return { ok: false, status: 0, desc: limpar(env, e).slice(0, 240), espera: 0 };
  }
}
async function enviarComRetentativa(env, metodo, corpo) {
  let r = await tg(env, metodo, corpo);
  if (!r.ok && (r.status === 0 || r.status >= 500) ) {
    await pausa(500);
    r = await tg(env, metodo, corpo);
  } else if (!r.ok && r.status === 429 && r.espera > 0 && r.espera <= 2) {
    await pausa(r.espera * 1000);
    r = await tg(env, metodo, corpo);
  }
  return r;
}
async function enviarAviso(env, html, threadId) {
  const corpo = {
    chat_id: env.TELEGRAM_CHAT_ID,
    text: html,
    parse_mode: "HTML",
    link_preview_options: { is_disabled: true },
  };
  if (threadId) corpo.message_thread_id = threadId;
  let r = await enviarComRetentativa(env, "sendMessage", corpo);
  if (!r.ok && r.status === 400 && /parse|entit|tag/i.test(r.desc)) {
    const semHtml = { ...corpo, text: html.replace(/<[^>]+>/g, "")
      .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&") };
    delete semHtml.parse_mode;
    r = await enviarComRetentativa(env, "sendMessage", semHtml);
  }
  return r;
}
async function criarTopico(env, nome) {
  const r = await tg(env, "createForumTopic", { chat_id: env.TELEGRAM_CHAT_ID, name: truncar(nome, 100) });
  if (!r.ok || !r.result || !r.result.message_thread_id) throw new Error(r.desc || `createForumTopic falhou (HTTP ${r.status})`);
  return r.result.message_thread_id;
}
async function renomearTopico(env, id, nome) {
  const r = await tg(env, "editForumTopic", {
    chat_id: env.TELEGRAM_CHAT_ID, message_thread_id: id, name: truncar(nome, 100),
  });
  if (r.ok || /not[ _]modified/i.test(r.desc)) return;
  throw new Error(r.desc || `editForumTopic falhou (HTTP ${r.status})`);
}
async function garantirTopico(env, topicos, p) {
  const reg = p.existente && p.existente.id ? p.existente : null;
  if (reg) {
    if (reg.nome !== p.nome) {
      try { await renomearTopico(env, reg.id, p.nome); reg.nome = p.nome; }
      catch (e) { console.warn("renomear tópico: " + limpar(env, e).slice(0, 160)); }
    }
    if (p.inicioMs && !reg.inicioMs) reg.inicioMs = p.inicioMs;
    reg.dia = p.dia || reg.dia;
    topicos[p.key] = reg;
    for (const k of p.migrarDe || []) delete topicos[k];
    return reg.id;
  }
  try {
    const id = await criarTopico(env, p.nome);
    topicos[p.key] = { id, nome: p.nome, inicioMs: p.inicioMs || 0, dia: p.dia };
    for (const k of p.migrarDe || []) delete topicos[k];
    return id;
  } catch (e) {
    console.error("criar tópico: " + limpar(env, e).slice(0, 160));
    return null; // fallback seguro: enviar mensagem no Geral sem perder a aposta
  }
}

// -----------------------------------------------------------------------------
// Agrupamento e mensagem de aposta
// -----------------------------------------------------------------------------
function agrupar(pendentes, agora = Date.now()) {
  const mapa = new Map();
  for (const it of pendentes) {
    if (Number(it.nextTryAt || 0) > agora) continue;
    const k = `${it.w}|${it.n.cond}|${it.n.outcome}|${it.n.side}`;
    if (!mapa.has(k)) mapa.set(k, { w: it.w, n: it.n, itens: [] });
    mapa.get(k).itens.push(it);
  }
  return [...mapa.values()];
}
function resumirGrupo(g) {
  let cotas = 0, valor = 0, ms = Infinity;
  for (const i of g.itens) {
    cotas += i.n.size;
    valor += i.n.size * i.n.price;
    if (i.n.ts) ms = Math.min(ms, i.n.ts * 1000);
  }
  return {
    cotas, valor, precoMedio: cotas ? valor / cotas : 0,
    ms: ms === Infinity ? Date.now() : ms, entradas: g.itens.length,
  };
}
function nomeCarteira(cfg, w) {
  const c = cfg.carteiras.find(x => x.w === w);
  if (c && c.nome) return truncar(c.nome, 32);
  return `${w.slice(0, 6)}…${w.slice(-4)}`;
}
function montarAviso({ p, g, s, descricao, carteira, temTopico }) {
  const verbo = g.n.side === "SELL" ? "VENDA" : "COMPRA";
  const icone = g.n.side === "SELL" ? "🔴" : "🟢";
  const linhas = [
    `🎯 <b>APOSTA DETECTADA</b>`,
    `👛 <b>Carteira:</b> ${esc(carteira)}`,
    `🕹 <b>Mercado:</b> ${esc(descricao.classificacao.rotulo)}`,
    `📌 <b>Escolha:</b> ${esc(descricao.escolha)}`,
    `${icone} <b>Ação:</b> ${verbo}`,
    `💵 <b>Preço médio:</b> ${pct(s.precoMedio)}`,
    `💰 <b>Valor negociado:</b> ${moeda(s.valor)}`,
    `📦 <b>Cotas:</b> ${quantidade(s.cotas)}`,
    `🕒 <b>Primeira execução:</b> ${horaSegBR(s.ms)}`,
  ];
  if (s.entradas > 1) linhas.push(`🧾 <b>Execuções agrupadas:</b> ${s.entradas}`);
  if (!temTopico) linhas.splice(1, 0, `🏟 <b>Partida:</b> ${esc(p.nome)}`);
  if (g.n.eventSlug) linhas.push(`🔗 <a href="https://polymarket.com/event/${encodeURIComponent(g.n.eventSlug)}">Abrir evento na Polymarket</a>`);
  return linhas.join("\n");
}
async function enviarGrupo(env, g, cfg, topicos, cache) {
  const p = await resolverPartida(env, g.n, topicos, cache);
  const carteira = nomeCarteira(cfg, g.w);
  const descricao = descreverAposta(g.n);
  const s = resumirGrupo(g);
  let thread = await garantirTopico(env, topicos, p);
  let r = await enviarAviso(env, montarAviso({ p, g, s, descricao, carteira, temTopico: !!thread }), thread);

  // Se o tópico tiver sido apagado, a aposta vai para Geral; a próxima tentativa recria o tópico.
  if (!r.ok && thread && r.status === 400 && /thread|topic/i.test(r.desc)) {
    delete topicos[p.key];
    thread = null;
    r = await enviarAviso(env, montarAviso({ p, g, s, descricao, carteira, temTopico: false }), null);
  }
  return { ok: r.ok, desc: r.ok ? "" : limpar(env, r.desc), status: r.status, retryAfter: r.espera || 0 };
}

// -----------------------------------------------------------------------------
// Resumo diário de apostas por carteira
// -----------------------------------------------------------------------------
function registrarAposta(estado, g) {
  const hoje = diaBR(Date.now());
  if (!estado.resumo || estado.resumo.dia !== hoje) {
    estado.resumo = { versao: 2, dia: hoje, msgId: null, apostas: {}, mudou: false };
  } else if (estado.resumo.versao !== 2) {
    // O formato antigo contava mercados únicos, não execuções. Mantém a mensagem existente,
    // mas reinicia a contagem precisa a partir da migração.
    estado.resumo = { versao: 2, dia: hoje, msgId: estado.resumo.msgId || null, apostas: {}, mudou: false };
  }
  const lista = Array.isArray(estado.resumo.apostas[g.w]) ? estado.resumo.apostas[g.w] : [];
  const conjunto = new Set(lista);
  for (const item of g.itens) if (item.id) conjunto.add(item.id);
  estado.resumo.apostas[g.w] = [...conjunto];
  estado.resumo.mudou = true;
}
function montarResumo(cfg, r) {
  const larg = 28;
  const linhas = Object.entries(r.apostas || {})
    .map(([w, lista]) => ({ nome: truncar(nomeCarteira(cfg, w), larg), n: Array.isArray(lista) ? new Set(lista).size : 0 }))
    .filter(x => x.n > 0)
    .sort((a, b) => b.n - a.n || a.nome.localeCompare(b.nome, "pt-BR"));
  const total = linhas.reduce((s, x) => s + x.n, 0);
  const tabela = [
    "Carteira".padEnd(larg) + "Apostas".padStart(8),
    "─".repeat(larg + 8),
    ...(linhas.length ? linhas.map(x => x.nome.padEnd(larg) + String(x.n).padStart(8)) : ["Nenhuma aposta registrada"]),
    "─".repeat(larg + 8),
    "TOTAL".padEnd(larg) + String(total).padStart(8),
  ].join("\n");
  return `<b>📊 RESUMO DIÁRIO · ${diaComAnoBR(r.dia)}</b>\n<pre>${esc(tabela)}</pre>\n<i>Contagem de execuções de aposta entregues pelo bot · Atualizado às ${horaBR(Date.now())}</i>`;
}
async function atualizarResumo(env, estado, cfg) {
  const r = estado.resumo;
  if (!r) return false;
  const corpo = {
    chat_id: env.TELEGRAM_CHAT_ID,
    text: montarResumo(cfg, r),
    parse_mode: "HTML",
    link_preview_options: { is_disabled: true },
  };
  if (r.msgId) {
    const e = await enviarComRetentativa(env, "editMessageText", { ...corpo, message_id: r.msgId });
    if (e.ok || /not[ _]modified/i.test(e.desc)) { r.mudou = false; return true; }
    if (/not found|can't be edited|message_id_invalid|message to edit/i.test(e.desc.toLowerCase())) r.msgId = null;
    else {
      console.warn("resumo diário: " + limpar(env, e.desc).slice(0, 160));
      return false; // deixa mudou=true para tentar novamente no próximo ciclo
    }
  }
  const topicoResumo = Number(env.TELEGRAM_TOPICO_RESUMO) || 0;
  let novo = await enviarComRetentativa(env, "sendMessage", topicoResumo
    ? { ...corpo, message_thread_id: topicoResumo }
    : corpo);
  if (!novo.ok && topicoResumo && novo.status === 400 && /thread|topic/i.test(novo.desc)) {
    novo = await enviarComRetentativa(env, "sendMessage", corpo); // tópico de resumo inválido: usar Geral
  }
  if (novo.ok && novo.result) {
    r.msgId = novo.result.message_id;
    r.mudou = false;
    return true;
  }
  console.warn("resumo diário não atualizado: " + limpar(env, novo.desc).slice(0, 160));
  return false;
}

// -----------------------------------------------------------------------------
// Configuração e persistência KV
// -----------------------------------------------------------------------------
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
  if (!Array.isArray(carteiras)) {
    throw new Error('payload inválido: esperado { "carteiras": [{ "w": "0x...", "nome": "..." }] }');
  }
  const vistos = new Set();
  const lista = [];
  let ignoradas = 0;
  for (const c of carteiras) {
    const w = String(c && c.w || "").trim().toLowerCase();
    if (!/^0x[a-f0-9]{40}$/.test(w) || vistos.has(w)) { ignoradas++; continue; }
    vistos.add(w);
    lista.push({ w, nome: String(c && c.nome || "").trim().slice(0, 40) });
  }
  // Só checa conflito quando o cliente informa de qual versão partiu (clientes antigos não enviam base).
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
async function lerEstado(env) {
  const raw = await env.CFG.get(KV_ESTADO);
  let e;
  if (raw) {
    try { e = JSON.parse(raw); } catch (_) { throw new Error("estado4 corrompido; interrompi para não sobrescrever a fila"); }
  } else {
    e = { pendentes: await migrarFilaAntiga(env), vistos: {}, ultimo: {}, coletas: {}, resumo: null, indiceCarteira: 0 };
  }
  e.pendentes = Array.isArray(e.pendentes) ? e.pendentes.filter(it => it && it.n && it.w) : [];
  e.vistos = e.vistos && typeof e.vistos === "object" ? e.vistos : {};
  e.ultimo = e.ultimo && typeof e.ultimo === "object" ? e.ultimo : {};
  e.coletas = e.coletas && typeof e.coletas === "object" ? e.coletas : {};
  e.resumo = e.resumo && typeof e.resumo === "object" ? e.resumo : null;
  e.indiceCarteira = Number.isInteger(e.indiceCarteira) && e.indiceCarteira >= 0 ? e.indiceCarteira : 0;
  return e;
}
async function salvarEstado(env, estado) {
  await env.CFG.put(KV_ESTADO, JSON.stringify(estado));
}
async function lerTopicos(env) {
  const raw = await env.CFG.get(KV_TOPICOS);
  if (!raw) return {};
  let v;
  try { v = JSON.parse(raw); } catch (_) { return null; }
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const out = {};
  for (const [k, val] of Object.entries(v)) {
    if (typeof val === "number" || /^\d+$/.test(String(val))) {
      out[k] = { id: Number(val), nome: "", inicioMs: 0, dia: "" };
    } else if (val && typeof val === "object" && Number(val.id) > 0) {
      out[k] = { id: Number(val.id), nome: String(val.nome || ""), inicioMs: Number(val.inicioMs || 0), dia: String(val.dia || "") };
    }
  }
  return out;
}
async function salvarTopicos(env, topicos) {
  const limite = diaBR(Date.now() - DIAS_TOPICO * 86400000);
  for (const [k, v] of Object.entries(topicos)) if (v && v.dia && v.dia < limite) delete topicos[k];
  await env.CFG.put(KV_TOPICOS, JSON.stringify(topicos));
}
async function pegarLock(env) {
  const v = await env.CFG.get(KV_LOCK);
  if (v && Date.now() - Number(v) < LOCK_MS) return false;
  await env.CFG.put(KV_LOCK, String(Date.now()), { expirationTtl: 120 });
  return true;
}
const soltarLock = env => env.CFG.delete(KV_LOCK);

function selecionarCarteiras(carteiras, estado) {
  if (carteiras.length <= CARTEIRAS_POR_CICLO) return carteiras;
  const inicio = estado.indiceCarteira % carteiras.length;
  const selecionadas = [];
  for (let i = 0; i < CARTEIRAS_POR_CICLO; i++) selecionadas.push(carteiras[(inicio + i) % carteiras.length]);
  estado.indiceCarteira = (inicio + selecionadas.length) % carteiras.length;
  return selecionadas;
}

// -----------------------------------------------------------------------------
// Coleta incremental com cursor persistente
// -----------------------------------------------------------------------------
async function coletarCarteira(c, estado) {
  const w = c.w;
  const agora = agoraS();
  const existentes = new Set(Array.isArray(estado.vistos[w]) ? estado.vistos[w] : []);
  // Também deduplica contra a fila atual caso a lista limitada de vistos já tenha rodado.
  const prefixoPendente = `${w}|`;
  for (const item of estado.pendentes) {
    if (item && item.w === w && typeof item.id === "string" && item.id.startsWith(prefixoPendente)) {
      existentes.add(item.id.slice(prefixoPendente.length));
    }
  }
  let job = estado.coletas[w];
  if (!job || typeof job !== "object") {
    const checkpoint = Number(estado.ultimo[w] || 0);
    job = {
      start: Math.max(1, checkpoint ? checkpoint - SOBREPOSICAO_S : agora - JANELA_INICIAL_S),
      end: agora,
      cursor: null,
    };
    estado.coletas[w] = job; // persistir a janela antes da chamada protege retomadas após falha
  }
  let novos = 0;
  let paginas = 0;
  let concluido = false;
  try {
    while (paginas < PAGINAS_POR_CARTEIRA_CICLO) {
      const cursorAntes = job.cursor || null;
      const r = await fetchDados(montarUrlTrades(w, job.start, job.end, cursorAntes), {
        headers: { Accept: "application/json" },
      });
      if (!r.ok) throw new Error(`Data API HTTP ${r.status}${r.status === 429 ? " (rate limited; cursor preservado)" : ""}`);
      const body = await r.json();
      const trades = listaDe(body);
      let filaCheia = false;
      for (const bruto of trades) {
        const n = normalizar(bruto);
        if (!n || !n.cond || !eEsports(n)) continue;
        const k = chaveTrade(n);
        if (existentes.has(k)) continue;
        if (existentes.has(chaveTradeAntiga(n))) {
          existentes.add(k); // migração silenciosa do formato de deduplicação anterior
          continue;
        }
        if (estado.pendentes.length >= MAX_PENDENTES) { filaCheia = true; break; }
        existentes.add(k);
        estado.pendentes.push({ id: `${w}|${k}`, w, n, tent: 0, nextTryAt: 0 });
        novos++;
      }
      paginas++;
      const next = body && body.pagination ? body.pagination.next_cursor : null;
      if (filaCheia) {
        // Repetir esta página depois é seguro: os itens já inseridos serão deduplicados.
        job.cursor = cursorAntes;
        break;
      }
      if (next) {
        job.cursor = String(next);
        // Uma página por carteira/ciclo mantém o processamento justo; o cursor continua salvo.
        if (paginas >= PAGINAS_POR_CARTEIRA_CICLO) break;
      } else {
        concluido = true;
        delete estado.coletas[w];
        estado.ultimo[w] = job.end;
        break;
      }
    }
  } finally {
    estado.vistos[w] = [...existentes].slice(-MAX_VISTOS);
    if (!concluido) estado.coletas[w] = job;
  }
  return { novos, paginas, cursorPendente: !concluido };
}
async function coletar(cfg, estado) {
  const alvos = selecionarCarteiras(cfg.carteiras, estado);
  let novos = 0, paginas = 0, erros = 0;
  await mapLimit(alvos, CONCORRENCIA, async c => {
    try {
      const r = await coletarCarteira(c, estado);
      novos += r.novos;
      paginas += r.paginas;
    } catch (e) {
      erros++;
      console.warn(`[${c.w.slice(0, 8)}] ${String(e && e.message || e).slice(0, 180)}`);
    }
  });
  return { novos, paginas, erros, consultadas: alvos.length };
}

// -----------------------------------------------------------------------------
// Carteira removida: as apostas dela ainda na fila deixam de ser enviadas e o cursor
// de coleta é descartado. "vistos" e "ultimo" ficam, para não reenviar apostas antigas
// se a carteira voltar a ser adicionada.
function podarCarteirasRemovidas(estado, cfg) {
  const ativas = new Set(cfg.carteiras.map(c => c.w));
  const antes = estado.pendentes.length;
  estado.pendentes = estado.pendentes.filter(it => ativas.has(it.w));
  for (const w of Object.keys(estado.coletas)) if (!ativas.has(w)) delete estado.coletas[w];
  return antes - estado.pendentes.length;
}

// Nome ou lista mudou: o resumo do dia é refeito no próximo ciclo, sem esperar nova aposta.
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

// Ciclo principal
// -----------------------------------------------------------------------------
async function ciclo(env, origem) {
  const resultado = {
    origem, carteiras: 0, consultadas: 0, novos: 0, enviados: 0, falhas: 0, pendentes: 0, erros: [],
  };
  const faltam = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "CFG"].filter(k => !env[k]);
  if (faltam.length) return { ...resultado, erros: ["faltam configurações: " + faltam.join(", ")] };

  let lockObtido = false;
  let estado = null;
  let topicos = null;
  try {
    lockObtido = await pegarLock(env);
    if (!lockObtido) return { ...resultado, ocupado: true };
    const cfg = await lerConfig(env);
    resultado.carteiras = cfg.carteiras.length;
    estado = await lerEstado(env);
    podarCarteirasRemovidas(estado, cfg);
    sincronizarResumo(estado, cfg);
    topicos = await lerTopicos(env);
    if (topicos === null) {
      resultado.erros.push("registro de tópicos ilegível; ciclo interrompido para evitar duplicações");
      return resultado;
    }

    if (estado.pendentes.length < MAX_PENDENTES && cfg.carteiras.length) {
      const coleta = await coletar(cfg, estado);
      resultado.novos = coleta.novos;
      resultado.consultadas = coleta.consultadas;
      if (coleta.erros) resultado.erros.push(`${coleta.erros} carteira(s) não puderam ser consultadas neste ciclo`);
    }

    const cacheEventos = new Map();
    const enviados = new Set();
    for (const g of agrupar(estado.pendentes).slice(0, MAX_AVISOS)) {
      try {
        const r = await enviarGrupo(env, g, cfg, topicos, cacheEventos);
        if (r.ok) {
          for (const it of g.itens) enviados.add(it.id);
          registrarAposta(estado, g);
          resultado.enviados++;
        } else {
          resultado.falhas++;
          const tentativas = Math.max(...g.itens.map(it => Number(it.tent || 0))) + 1;
          const atraso = Math.max(r.retryAfter || 0, Math.min(300, 5 * (2 ** Math.min(tentativas, 6))));
          for (const it of g.itens) {
            it.tent = (it.tent || 0) + 1;
            it.nextTryAt = Date.now() + atraso * 1000;
          }
          resultado.erros.push(truncar(r.desc || `falha no Telegram (${r.status})`, 220));
        }
      } catch (e) {
        resultado.falhas++;
        for (const it of g.itens) {
          it.tent = (it.tent || 0) + 1;
          it.nextTryAt = Date.now() + Math.min(300, 5 * (2 ** Math.min(it.tent, 6))) * 1000;
        }
        resultado.erros.push(truncar(limpar(env, e), 220));
      }
    }

    // Falhas ficam pendentes, sem limite de tentativas que descarte a aposta.
    estado.pendentes = estado.pendentes.filter(it => !enviados.has(it.id));
    if (estado.resumo && estado.resumo.mudou) await atualizarResumo(env, estado, cfg);
    resultado.pendentes = estado.pendentes.length;
    resultado.proximoCursor = Object.keys(estado.coletas).length;
  } catch (e) {
    resultado.erros.push(truncar(limpar(env, e), 300));
  } finally {
    try {
      if (estado) await salvarEstado(env, estado);
      if (topicos) await salvarTopicos(env, topicos);
    } catch (e) {
      resultado.erros.push("persistência KV: " + truncar(limpar(env, e), 200));
    }
    if (lockObtido) {
      try { await soltarLock(env); } catch (e) { resultado.erros.push("liberação da trava: " + truncar(limpar(env, e), 120)); }
    }
  }
  return resultado;
}

// -----------------------------------------------------------------------------
// HTTP e cron
// -----------------------------------------------------------------------------
async function salvarCarteirasPayload(env, body) {
  return salvarCarteiras(env, body && body.carteiras, body && body.base);
}
function autorizado(req, env, body) {
  if (!env.CHAVE) return false;
  const auth = req.headers.get("Authorization") || "";
  const token = auth.replace(/^Bearer\s+/i, "").trim()
    || (body && typeof body.chave === "string" ? body.chave : "");
  return token === env.CHAVE;
}

export default {
  async fetch(req, env) {
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    const path = new URL(req.url).pathname;
    if (req.method === "GET" && path === "/") return textResp("ok");
    if (req.method === "GET" && path === "/config") {
      if (!autorizado(req, env, null)) return jsonResp({ ok: false, erro: "não autorizado" }, 401);
      try {
        const cfg = await lerConfig(env);
        return jsonResp({ ok: true, carteiras: cfg.carteiras, atualizado: cfg.atualizado });
      } catch (e) {
        return jsonResp({ ok: false, erro: truncar(limpar(env, e), 300) }, 400);
      }
    }
    if (req.method !== "POST" || (path !== "/config" && path !== "/notify")) return textResp("não encontrado", 404);

    let body = null;
    try { body = await req.json(); } catch (_) {
      return jsonResp({ ok: false, erro: "corpo JSON inválido" }, 400);
    }
    if (!autorizado(req, env, body)) return jsonResp({ ok: false, erro: "não autorizado" }, 401);

    try {
      if (path === "/config") return jsonResp({ ok: true, ...(await salvarCarteirasPayload(env, body)) });
      const out = { ok: true };
      if (body && Array.isArray(body.carteiras)) Object.assign(out, await salvarCarteirasPayload(env, body));
      out.ciclo = await ciclo(env, "manual");
      return jsonResp(out);
    } catch (e) {
      if (e && e.conflito) {
        const atual = await lerConfig(env).catch(() => ({ carteiras: [], atualizado: null }));
        return jsonResp({ ok: false, conflito: true, erro: "lista alterada em outro aparelho",
          carteiras: atual.carteiras, atualizado: atual.atualizado }, 409);
      }
      return jsonResp({ ok: false, erro: truncar(limpar(env, e), 300) }, 400);
    }
  },
  async scheduled(_event, env) {
    await ciclo(env, "cron");
  },
};

