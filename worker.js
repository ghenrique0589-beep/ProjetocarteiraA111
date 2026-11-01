// Polymarket -> Telegram (e-sports)
// Module Worker (Cloudflare Workers) - v2.3
// Cron recomendado: * * * * *
//
// Secrets / variáveis (Worker Settings > Variables and Secrets):
//   CHAVE                  token privado para proteger POST /config e POST /notify
//   TELEGRAM_BOT_TOKEN     token do bot; o bot deve ser administrador com gestão de tópicos
//   TELEGRAM_CHAT_ID       ID do supergrupo com tópicos ativados
//   TELEGRAM_TOPICO_RESUMO (opcional) ID do tópico do resumo diário
//   DRY_RUN=1              (opcional) simula o Telegram: nada é enviado, o estado é gravado normalmente.
//                          Use só em Worker/KV de teste.
//   CARTEIRAS_POR_CICLO    (opcional, padrão 20) carteiras consultadas por minuto
//   SUBREQ_LIMITE          (opcional, padrão 45) orçamento de requisições por execução.
//                          Plano grátis: limite de 50. Workers Paid: use 900.
//   CONCORRENCIA           (opcional, padrão 4) carteiras consultadas em paralelo
//   LIMITE_FOGO            (opcional, padrão 1000) aposta (US$) a partir da qual o aviso ganha 🔥
//   PRECO_EM_CENTAVOS=1    (opcional) mostra o preço como 59¢ em vez de 59%
//   RANKING=0              (opcional) desliga o tópico "🏆 Apostas" (ranking do dia; ligado por padrão)
//   RANKING_MINIMO         (opcional, padrão 10) aposta mínima (US$) para entrar no ranking; 0 conta tudo
//   RANKING_INTERVALO_S    (opcional, padrão 120) intervalo mínimo entre edições do ranking
//   TELEGRAM_TOPICO_RANKING (opcional) ID de um tópico já existente para o ranking
//   MOSTRAR_VERSAO=1       (opcional) acrescenta a versão no fim de cada aviso (útil para comparar Workers)
//
// Binding KV (namespace binding name: CFG). Para testar em paralelo, use um namespace SEPARADO:
//   CFG / CFG                  configuração das carteiras
//   CFG / estado4              fila, cursores, deduplicação, tópicos, resumo do dia, eventos
//   CFG / lista_carteiras      mensagens da lista "CARTEIRAS MONITORADAS" (ids, hash, dia)
//   CFG / topico_atualizacoes  ID do tópico "Atualizações"
//   CFG / estado3, topicos     legado: lidos só na migração, nunca gravados
//
// Gravações no KV (plano grátis: 1.000 por dia, renovam à meia-noite UTC):
//   - estado4 só é gravado quando algo muda de verdade, ou no checkpoint (15 min);
//   - rodada com aviso pronto grava ANTES de enviar: se essa gravação falhar, nada é enviado;
//   - depois de enviar, grava de novo só se o estado mudou (tira da fila o que saiu).
// Sem trava no KV: cada rodada tem prazo (PRAZO_*) para terminar antes da próxima.
//
// Endpoints: GET / · GET /status · GET /config · POST /config · POST /notify
//   POST /notify com {"simular":true} (ou ?simular=1) roda uma rodada de pré-visualização:
//   não envia nada ao Telegram e não grava nada no KV; devolve o que seria enviado.

const DATA_API = "https://data-api.polymarket.com";
const GAMMA_API = "https://gamma-api.polymarket.com";
const FUSO = "America/Sao_Paulo";

const KV_CFG = "CFG";
const KV_ESTADO = "estado4";
const KV_ESTADO_ANTIGO = "estado3";
const KV_TOPICOS_LEGADO = "topicos";
const KV_LISTA = "lista_carteiras";
const KV_TOPICO_ATUALIZACOES = "topico_atualizacoes";

const VERSAO = "2026-11-01-v2.3";
const VERSAO_CURTA = "v2.3";
const NOME_TOPICO_ATUALIZACOES = "Atualizações";
const NOME_TOPICO_RANKING = "🏆 Apostas";

const ESPORT = /(?:^|[-_\s/])(?:league[-_\s]?of[-_\s]?legends|lol|counter[-_\s]?strike|cs2|csgo|cs-go|dota[-_\s]?2?|valorant|rainbow[-_\s]?six|r6siege|r6|call[-_\s]?of[-_\s]?duty|cod(?:mw\d?)?|overwatch|ow2|rocket[-_\s]?league|rlcs|starcraft|mobile[-_\s]?legends|mlbb|honor[-_\s]?kings|hok|king[-_\s]?glory|kog|wild[-_\s]?rift|arena[-_\s]?of[-_\s]?valor)(?:$|[-_\s/])/i;

const PADRAO_CARTEIRAS_POR_CICLO = 20;
const PADRAO_SUBREQ_LIMITE = 45;
const PADRAO_CONCORRENCIA = 4;
const RESERVA_ENVIO = 16;          // requisições guardadas para a fase de envio
const CUSTO_GRUPO = 7;             // pior caso de um aviso: Gamma(2) + tópico + placar + mensagem + edição do placar
const LIMITE_PAGINA = 500;
const SOBREPOSICAO_S = 300;
const JANELA_INICIAL_S = 3600;     // carteira nova: quanto do passado enviar
const JANELA_MAXIMA_S = 3 * 3600;  // depois de uma falha longa, não recupera mais que isso
const JOB_MAX_IDADE_S = 2 * 3600;  // coleta parada há mais que isso é descartada e recomeça
const MAX_PENDENTES = 3000;
const MAX_AVISOS = 6;
const MAX_TENTATIVAS = 8;          // falha passageira: depois disso, o aviso é descartado (fica no log)
const DIAS_TOPICO = 7;             // tópico sem uso por tantos dias sai do estado
const TIMEOUT_FETCH_MS = 8000;
const TENTATIVAS_METADADOS = 3;
const RETRY_METADADOS_S = 30;
const RETRY_CONFIG_S = 600;        // erro de configuração do Telegram: tenta de novo em 10 min, sem descartar
const EV_VALIDADE_MS = 60 * 60 * 1000; // dados do evento (Gamma): renovam a cada 1 h
const EV_FALHA_MS = 5 * 60 * 1000;     // evento não encontrado: tenta de novo em 5 min
const CHECKPOINT_MS = 15 * 60 * 1000;
const PRAZO_COLETA_MS = 20000;
const PRAZO_ENVIO_MS = 35000;
const ESPERA_MAX_429_S = 15;       // espera do Telegram acima disso vai para a fila, não para a rodada
const LIMITE_BLOCO = 3500;         // lista de carteiras: tamanho máximo de cada mensagem
const LIMITE_TEXTO = 3800;         // corpo de mensagens variáveis (o Telegram aceita 4096)
const LISTA_RETRY_MS = 5 * 60 * 1000;
const STATUS_MAX_IDADE_MIN = 20;

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
const chaveTexto = v => String(v ?? "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
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
const pad2 = n => String(n).padStart(2, "0");

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

// ───────────────────────── descrição da aposta ─────────────────────────

const alvoMercado = n => `${n.titulo || ""} ${String(n.slug || "").replace(/[-_]+/g, " ")}`.replace(/\s+/g, " ").trim();
const RE_TOTAL_MAPAS = /\btotal\s*(?:(?:de|of)\s*)?(?:maps?|mapas|games)\b|\b(?:maps?|mapas|games)\s*(?:totais?|total|over|under|o\s*\/\s*u)\b|\b(?:over\s*\/\s*under|o\s*\/\s*u|over|under)\s*(?:\d+(?:[.,]\d+)?)?\s*(?:(?:de|of)\s*)?(?:maps?|mapas|games)\b|\b(?:number\s+of\s+maps|maps?\s+played)\b/i;
const SUBTIPOS_OU = [
  [/\bkills?\b/i, "kills"], [/\brounds?\b/i, "rodadas"], [/\b(?:towers?|turrets?)\b/i, "torres"],
  [/\bdragons?\b/i, "dragões"], [/\bbarons?\b/i, "barões"], [/\binhibitors?\b/i, "inibidores"],
  [/\bpoints?\b/i, "pontos"], [/\b(?:duration|minutes?)\b/i, "minutos"],
];
const OUTRAS_PALAVRAS = /\b(odd|even|overtime|pistol|ace|knife|clutch|bomb|plant|rounds?|kills?|total|score|first|last)\b/i;

function numeroMapaDe(texto) {
  const m = String(texto || "").match(/\b(?:map|mapa|game|jogo)\s*#?\s*(\d{1,2})\b(?![.,]\d)/i);
  return m ? Number(m[1]) : 0;
}

function linhaTotal(n) {
  let limpo = `${n.titulo || ""} ${String(n.slug || "").replace(/\d{4}-\d{2}-\d{2}/g, " ")}`;
  limpo = limpo.replace(/[-_]+/g, " ").replace(/(\d)pt(\d)/gi, "$1.$2");
  limpo = limpo.replace(/\bbo\s*\d+\b/gi, " ");
  limpo = limpo.replace(/\b(?:map|mapa|maps|mapas|game|jogo)\s*#?\s*\d{1,2}\b(?![.,]\d)/gi, " ");
  const decimal = limpo.match(/\b\d{1,3}[.,]\d+\b/);
  if (decimal) return decimal[0].replace(".", ",");
  const inteiro = limpo.match(/\b\d{1,2}\b/);
  return inteiro ? inteiro[0] : "";
}

const igualTexto = (a, b) => {
  const x = chaveTexto(a), y = chaveTexto(b);
  return !!x && !!y && (x === y || x.endsWith(" " + y) || y.endsWith(" " + x));
};
const fmtLinha = v => `${v > 0 ? "+" : ""}${String(v).replace(".", ",")}`;

// Linha do handicap para o lado que a pessoa apostou. Devolve { time, valor } ou null se não der para ter certeza.
function linhaHandicap(n, o) {
  // 1) a linha pode vir no próprio resultado: "NAVI -1.5" ou "NAVI (-1.5)"
  const direto = o.match(/(?:^|[\s(])([+-]\d+(?:[.,]\d+)?)\)?\s*$/);
  if (direto) return { time: o.replace(direto[0], "").trim() || o, valor: Number(direto[1].replace(",", ".")) };
  // 2) pares "Time (±linha)" no título: o lado apostado leva a linha do próprio time; o outro lado, a oposta
  const tit = String(n.titulo || "");
  let achados = [...tit.matchAll(/([^():]+?)\s*\(\s*([+-]\d+(?:[.,]\d+)?)\s*\)/g)];
  if (!achados.length) achados = [...tit.matchAll(/([^():+\/]*?[A-Za-z0-9.])\s+([+-]\d+(?:[.,]\d+)?)(?![\d.,])/g)]; // "Time -1.5" sem parênteses
  const pares = achados.map(x => ({
    time: x[1].trim().replace(/^(?:vs\.?|versus)\s+/i, "").replace(/^[\/,;|\s]+/, ""),
    valor: Number(x[2].replace(",", ".")),
  }));
  const meu = pares.find(pr => igualTexto(pr.time, o));
  if (meu) return { time: o, valor: meu.valor };
  const outros = pares.filter(pr => !igualTexto(pr.time, o));
  if (pares.length === 1 || (pares.length === 2 && outros.length === 1)) return { time: o, valor: -outros[0].valor };
  return null;
}

// Rótulo do mercado e escolha, em português, sem ambiguidade entre partida e mapa.
// "ordem" organiza o placar (partida primeiro, depois cada mapa). "conhecido=false" vai para o log.
function descreverAposta(n) {
  const o = String(n.outcome || "?").trim();
  const ol = o.toLowerCase();
  const titulo = String(n.titulo || "");
  const limpo = alvoMercado(n).replace(/(\d)pt(\d)/gi, "$1.$2");
  const mapa = numeroMapaDe(limpo);
  const pref = mapa ? `Mapa ${mapa} · ` : "";
  const base = mapa ? 10 + mapa * 10 : 4;
  const res = (tipo, rotulo, escolha, ordem, conhecido = true) => ({ tipo, rotulo, escolha, ordem, conhecido });
  const simNao = ol === "yes" ? "Sim" : ol === "no" ? "Não" : null;
  const ou = o.match(/^(over|under)(?:\s+(\d+(?:[.,]\d+)?))?\s*$/i);

  if (simNao) {
    const q = titulo.match(/\bwill\s+(.+?)\s+win\b/i);
    if (q) return res("sim_nao", mapa ? `Mapa ${mapa}` : "Partida", `${q[1].trim()} (${simNao})`, mapa ? base : 0);
    return res("sim_nao", truncar(titulo || n.slug || "Mercado", 60), simNao, 200, false);
  }
  if (ou) {
    const lado = ou[1].toLowerCase() === "over" ? "Mais de" : "Menos de";
    const linha = (ou[2] ? ou[2].replace(".", ",") : "") || linhaTotal(n);
    const escolha = `${lado} ${linha || "(linha não identificada)"}`;
    if (RE_TOTAL_MAPAS.test(limpo)) return res("total_mapas", "Total de mapas", escolha, 2);
    for (const [re, nome] of SUBTIPOS_OU) if (re.test(limpo)) return res("ou", `${pref}Total de ${nome}`, escolha, base + 2);
    return res("ou", truncar(titulo || n.slug || "Mercado", 60), escolha, 200, false);
  }
  if (/\b(handicap|spread)\b/i.test(limpo)) {
    const rotulo = mapa ? `Handicap mapa ${mapa}` : "Handicap partida";
    const ordem = mapa ? base + 1 : 1;
    const h = linhaHandicap(n, o);
    return h
      ? res("handicap", rotulo, `${h.time} (${fmtLinha(h.valor)})`, ordem)
      : res("handicap", rotulo, `${o} (linha não identificada)`, ordem, false);
  }
  if (/\b(correct\s+score|exact\s+score|placar\s+(?:correto|exato)|series\s+score|map\s+score)\b/i.test(limpo)) {
    return res("placar", mapa ? `Placar mapa ${mapa}` : "Placar partida", o, mapa ? base + 3 : 3);
  }
  const f = limpo.match(/\b(?:first\s+(blood|kill|tower|turret|dragon|baron|inhibitor|roshan)|(primeiro\s+sangue))\b/i);
  if (f) {
    const nome = f[2] ? "First Blood" : `First ${f[1][0].toUpperCase()}${f[1].slice(1).toLowerCase()}`;
    return res("primeiro", `${pref}${nome}`, o, base + 3);
  }
  if (mapa) return res("mapa", `Mapa ${mapa}`, o, base);
  const times = extrairTimes(titulo);
  const lado = times && (igualTexto(o, times.a) || igualTexto(o, times.b));
  const rotuloPartida = /\(\s*bo\s*\d\s*\)/i.test(titulo) && !OUTRAS_PALAVRAS.test(titulo.replace(/counter-strike/ig, ""));
  if ((times && (lado || rotuloPartida)) || /\b(match\s+winner|winner\s+of\s+(?:the\s+)?match|match\s+result|moneyline|series\s+winner|who\s+will\s+win|win\s+(?:the\s+)?match|vencedor\s+da\s+partida)\b/i.test(limpo)) {
    return res("vencedor", "Partida", o, 0);
  }
  return res("outro", truncar(titulo || n.slug || "Mercado", 60), o, 200, false);
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

// ───────────────────────── infraestrutura ─────────────────────────

// Memória do isolate (não é gravada em lugar nenhum; só reduz leituras e protege contra reenvio).
const CACHE = { topicoAtualizacoes: 0, listaOk: null, listaFalhaAte: 0, recentes: new Map(), desconhecidos: new Set(), rankingEdicao: {} };
let SEQ_DRY = 0;

const num = (env, nome, padrao) => {
  const v = Number(env && env[nome]);
  return Number.isFinite(v) && v > 0 ? v : padrao;
};
const ligado = v => /^(1|true|sim|yes|on)$/i.test(String(v ?? "").trim());

// Cada execução ganha seu próprio contexto: herda os bindings do env e leva contadores e modo de teste.
// As funções abaixo recebem este contexto no lugar do env.
function criarCtx(env, origem, opcoes = {}) {
  const ctx = Object.create(env || {});
  ctx.run = {
    origem, inicio: Date.now(),
    sub: 0, kvLeitura: 0, kvGravacao: 0, polymarket: 0, gamma: 0, telegram: 0,
    limite: num(env, "SUBREQ_LIMITE", PADRAO_SUBREQ_LIMITE),
    simular: !!opcoes.simular,                       // pré-visualização: sem Telegram e sem gravações
    dry: ligado(env && env.DRY_RUN) || !!opcoes.simular, // Telegram simulado
    semGravar: !!opcoes.simular,                      // KV só leitura
    simuladas: [], limitado429: false, estadoBytes: 0, latencias: [], desconhecidos: [],
  };
  return ctx;
}

async function kvGet(env, chave) {
  env.run.kvLeitura++; env.run.sub++;
  return env.CFG.get(chave);
}
async function kvPut(env, chave, valor) {
  if (env.run.semGravar) return;
  env.run.kvGravacao++; env.run.sub++;
  return env.CFG.put(chave, valor);
}

// Toda chamada externa passa por aqui: conta no orçamento de requisições e aplica o prazo.
async function http(env, tipo, url, init = {}, timeoutMs = TIMEOUT_FETCH_MS) {
  env.run[tipo]++; env.run.sub++;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort("tempo limite da requisição"), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}
async function fetchDados(env, url, init = {}) {
  let ultimaFalha;
  for (let tentativa = 0; tentativa < 2; tentativa++) {
    try {
      const r = await http(env, "polymarket", url, init);
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

// IDs do Telegram. Em modo simulado os IDs são negativos; em modo real, só IDs positivos valem.
const idTelegram = (env, v) => {
  const n = Number(v);
  return Number.isInteger(n) && (env.run.dry ? n !== 0 : n > 0) ? n : null;
};

const METODOS_ESCRITA = new Set(["sendMessage", "editMessageText", "createForumTopic", "deleteMessage"]);

// Envia ao Telegram. Espera longa (429) não é dormida aqui: volta com retryAfter para a fila.
async function enviarComRetentativa(env, metodo, corpo, tentativas = 3) {
  if (env.run.dry && METODOS_ESCRITA.has(metodo)) {
    const id = -(1000 + (++SEQ_DRY));
    const texto = String(corpo.text || corpo.name || "");
    if (env.run.simuladas.length < 30) {
      env.run.simuladas.push({ metodo, topico: corpo.message_thread_id ?? null, texto: truncar(texto, 4000) });
    }
    console.log(`[SIMULADO] ${metodo} ${JSON.stringify({ topico: corpo.message_thread_id ?? null, texto: truncar(texto, 300) })}`);
    return { ok: true, status: 200, dry: true, result: { message_id: id, message_thread_id: id } };
  }
  let ultima = { ok: false, status: 0, desc: "falha desconhecida" };
  for (let i = 0; i < tentativas; i++) {
    try {
      const r = await http(env, "telegram", `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${metodo}`, {
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
      ultima = { ok: false, status: 0, desc: limpar(env, e) };
      if (i < tentativas - 1) await pausa(300 * (i + 1));
    }
  }
  return ultima;
}

// transitoria: tenta de novo (até MAX_TENTATIVAS) · config: token/chat/permissão, espera e não descarta
// permanente: o Telegram recusou esta mensagem; repetir não adianta.
const RE_CONFIG = /chat not found|bot was kicked|bot is not a member|not enough rights|have no rights|can't send|CHAT_WRITE_FORBIDDEN|TOPIC_CLOSED|CHAT_ADMIN_REQUIRED|forbidden|unauthorized/i;
function classificarFalha(r) {
  const s = Number((r && r.status) || 0);
  const d = String((r && r.desc) || "");
  if (s === 401 || s === 403 || s === 404 || RE_CONFIG.test(d)) return "config";
  if (s === 400) return "permanente";
  return "transitoria";
}

function textoPuro(html) {
  return String(html || "").replace(/<[^>]+>/g, "").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");
}
// Se o Telegram recusar o HTML, reenvia a mesma mensagem como texto simples.
async function enviarTexto(env, corpo) {
  let r = await enviarComRetentativa(env, "sendMessage", corpo);
  if (!r.ok && r.status === 400 && /parse entities|can't parse/i.test(r.desc || "")) {
    r = await enviarComRetentativa(env, "sendMessage", { ...corpo, text: textoPuro(corpo.text), parse_mode: undefined });
  }
  return r;
}

// Junta blocos de texto sem passar do limite; o que não couber vira "… e mais N".
function juntarBlocos(blocos, sep, limite, rotuloResto) {
  const out = [];
  let tam = 0;
  for (let i = 0; i < blocos.length; i++) {
    const b = blocos[i];
    if (tam + b.length + sep.length > limite) { out.push(rotuloResto(blocos.length - i)); break; }
    out.push(b);
    tam += b.length + sep.length;
  }
  return out.join(sep);
}

function nomeCarteira(cfg, w) {
  const c = cfg.carteiras.find(x => x.w === w);
  return (c && c.nome) || w.slice(0, 8) + "…";
}

// ───────────────────────── lista de carteiras ─────────────────────────

async function garantirTopicoAtualizacoes(env) {
  if (CACHE.topicoAtualizacoes > 0 && !env.run.dry) return CACHE.topicoAtualizacoes;
  const salvo = idTelegram(env, await kvGet(env, KV_TOPICO_ATUALIZACOES));
  if (salvo) {
    if (salvo > 0) CACHE.topicoAtualizacoes = salvo;
    return salvo;
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
  await kvPut(env, KV_TOPICO_ATUALIZACOES, String(id));
  if (id > 0) CACHE.topicoAtualizacoes = id;
  return id;
}

// A lista é dividida em várias mensagens: o Telegram recusa texto acima de 4096 caracteres.
function blocosLista(cfg) {
  const total = cfg.carteiras.length;
  const rodape = `<code>${VERSAO}</code> · atualizado ${horaSegBR(Date.now())}`;
  if (!total) {
    return [`📋 <b>CARTEIRAS MONITORADAS</b> · 0\n\n<i>Nenhuma carteira no momento.</i>\n\n${rodape}`];
  }
  const entradas = cfg.carteiras.map((c, i) => {
    const nome = c.nome ? esc(c.nome) : "<i>sem nome</i>";
    return `<b>${pad2(i + 1)}.</b> ${nome}\n<code>${c.w}</code>`;
  });
  const grupos = [];
  let atual = [];
  let tam = 0;
  for (const e of entradas) {
    if (atual.length && tam + e.length + 2 > LIMITE_BLOCO) { grupos.push(atual); atual = []; tam = 0; }
    atual.push(e);
    tam += e.length + 2;
  }
  if (atual.length) grupos.push(atual);
  return grupos.map((g, i) => {
    const cab = i === 0
      ? `📋 <b>CARTEIRAS MONITORADAS</b> · ${total}`
      : `📋 <b>CARTEIRAS</b> · continuação ${i + 1}/${grupos.length}`;
    const fim = i === grupos.length - 1 ? `\n\n${rodape}` : "";
    return `${cab}\n\n${g.join("\n\n")}${fim}`;
  });
}

async function lerLista(env, legado) {
  const raw = await kvGet(env, KV_LISTA);
  let v = null;
  try { v = raw ? JSON.parse(raw) : null; } catch (_) { v = null; }
  let ids = [];
  if (v && Array.isArray(v.ids)) ids = v.ids;
  else if (v && v.msgId) ids = [v.msgId];
  else if (legado && legado.msgId) ids = [legado.msgId];
  const base = v || legado || {};
  return {
    ids: ids.map(Number).filter(x => Number.isInteger(x) && x !== 0),
    hash: String(base.hash || ""),
    dia: String(base.dia || ""),
  };
}

// A lista fica em chave própria: não mexe no estado da fila (menos gravações e sem corrida com o cron).
async function atualizarListaCarteiras(env, cfg, forcar = false, legado = null) {
  const hash = cfg.carteiras.map(c => c.w + "|" + (c.nome || "")).join(",");
  const dia = diaBR(Date.now());
  const c = CACHE.listaOk;
  if (!forcar && c && c.hash === hash && c.dia === dia) return false;
  if (!forcar && Date.now() < CACHE.listaFalhaAte) return false;

  const lista = await lerLista(env, legado);
  if (!forcar && lista.hash === hash && lista.dia === dia) {
    if (!env.run.simular) CACHE.listaOk = { hash, dia };
    return false;
  }
  const topicoId = await garantirTopicoAtualizacoes(env);
  if (!topicoId) { CACHE.listaFalhaAte = Date.now() + LISTA_RETRY_MS; return false; }

  const blocos = blocosLista(cfg);
  const finais = lista.ids.map(x => idTelegram(env, x));
  let falhou = false;
  for (let i = 0; i < blocos.length; i++) {
    const corpo = {
      chat_id: env.TELEGRAM_CHAT_ID, text: blocos[i], parse_mode: "HTML",
      disable_web_page_preview: true, message_thread_id: topicoId,
    };
    if (finais[i]) {
      const e = await enviarComRetentativa(env, "editMessageText", { ...corpo, message_id: finais[i] });
      if (e.ok || /not[ _]modified/i.test(e.desc || "")) continue;
      if (!/message to edit not found|message_id_invalid|can't be edited/i.test(e.desc || "")) {
        console.warn("lista carteiras (edit): " + limpar(env, e.desc).slice(0, 160));
        falhou = true;
        break;
      }
      finais[i] = null; // mensagem apagada no Telegram: envia uma nova
    }
    const s = await enviarComRetentativa(env, "sendMessage", corpo);
    if (s.ok && s.result) {
      finais[i] = s.result.message_id;
    } else {
      console.warn("lista carteiras (send): " + limpar(env, s.desc).slice(0, 160));
      if (s.status === 400 && /thread|topic/i.test(s.desc || "")) {
        // tópico "Atualizações" apagado: esquece o ID para recriar na próxima tentativa
        CACHE.topicoAtualizacoes = 0;
        await kvPut(env, KV_TOPICO_ATUALIZACOES, "0");
      }
      falhou = true;
      break;
    }
  }
  if (!falhou) {
    for (let j = blocos.length; j < finais.length; j++) {
      if (finais[j]) await enviarComRetentativa(env, "deleteMessage", { chat_id: env.TELEGRAM_CHAT_ID, message_id: finais[j] });
    }
    finais.length = blocos.length;
  }
  const ids = finais.filter(Boolean);
  const mudouIds = JSON.stringify(ids) !== JSON.stringify(lista.ids);
  if (!falhou || mudouIds) {
    await kvPut(env, KV_LISTA, JSON.stringify({
      ids, hash: falhou ? lista.hash : hash, dia: falhou ? lista.dia : dia,
    }));
  }
  if (falhou) { CACHE.listaFalhaAte = Date.now() + LISTA_RETRY_MS; return false; }
  if (!env.run.simular) CACHE.listaOk = { hash, dia };
  return true;
}

// ───────────────────────── resumo diário ─────────────────────────

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
  const ordem = cfg.carteiras.map(c => c.w).filter(w => r.apostas && r.apostas[w]);
  const linhas = ordem.map((w, i) => {
    const nome = (r.nomes && r.nomes[w]) || nomeCarteira(cfg, w);
    return `👤 <b>${pad2(i + 1)} · ${esc(nome)}:</b> ${r.apostas[w]} aposta(s)`;
  });
  const total = Object.values(r.apostas || {}).reduce((a, b) => a + b, 0);
  const lista = juntarBlocos(linhas, "\n", LIMITE_TEXTO - 400, n => `… e mais ${n} carteira(s)`);
  const texto = `📊 <b>RESUMO DIÁRIO · ${diaComAnoBR(r.dia)}</b>\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `${lista || "— nenhuma aposta —"}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `💰 <b>TOTAL:</b> ${total} aposta(s)\n\n` +
    `<i>Atualizado às ${horaBR(Date.now())} · ${VERSAO}</i>`;
  const corpo = { chat_id: env.TELEGRAM_CHAT_ID, text: texto, parse_mode: "HTML", disable_web_page_preview: true };
  const msgId = idTelegram(env, r.msgId);
  if (msgId) {
    const e = await enviarComRetentativa(env, "editMessageText", { ...corpo, message_id: msgId });
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

// ───────────────────────── ranking do dia (tópico "🏆 Apostas") ─────────────────────────
// Conta pela hora em que a aposta foi feita (fuso de São Paulo), não pela data da partida: aposta de hoje
// num jogo de amanhã entra no ranking de hoje. Vendas ficam fora do volume e aparecem numa linha à parte.

const PADRAO_RANKING_MINIMO = 10;
const PADRAO_RANKING_INTERVALO_S = 120;
const GRACA_RANKING_MS = 3 * 3600 * 1000;   // aposta de ontem vista só depois da meia-noite ainda cai no dia certo
const MAX_RANKING_PARTIDAS = 80;
const MAX_RANKING_POSICOES = 120;

const rankingLigado = env => !/^(0|false|nao|não|no|off)$/i.test(String(env.RANKING ?? "1").trim());
function rankingMinimo(env) {
  const bruto = env.RANKING_MINIMO;
  const v = Number(bruto);
  return bruto !== undefined && String(bruto).trim() !== "" && Number.isFinite(v) && v >= 0 ? v : PADRAO_RANKING_MINIMO;
}
const novoRanking = dia => ({ dia, msgId: null, mudou: false, partidas: {}, pos: {}, carteiras: {}, vd: 0 });

// Virou o dia: o ranking de ontem fica guardado por algumas horas só para receber apostas que chegaram atrasadas.
function rolarRanking(estado) {
  const hoje = diaBR(Date.now());
  if (estado.ranking && estado.ranking.dia !== hoje) {
    estado.rankingAnt = estado.ranking;
    estado.rankingAnt.ate = Date.now() + GRACA_RANKING_MS;
    estado.ranking = null;
  }
  const a = estado.rankingAnt;
  if (a && Date.now() > Number(a.ate || 0) && !a.mudou) estado.rankingAnt = null;
}

function podarMapa(mapa, max) {
  const chaves = Object.keys(mapa);
  if (chaves.length <= max) return;
  chaves.sort((a, b) => Number(mapa[b].v) - Number(mapa[a].v));
  for (const k of chaves.slice(max)) delete mapa[k];
}

function registrarNoRanking(env, estado, chave, topico, g, linhas) {
  const hoje = diaBR(Date.now());
  // cada execução vale pelo dia em que foi feita; grupos que cruzam a meia-noite são separados por dia
  const porDia = new Map();
  for (const it of g.itens) {
    const ms = Number(it.n.ts || 0) * 1000;
    const d = ms > 0 ? diaBR(ms) : hoje;
    if (!porDia.has(d)) porDia.set(d, []);
    porDia.get(d).push(it);
  }
  for (const [dia, itens] of porDia) {
    let r;
    if (dia === hoje) r = estado.ranking || (estado.ranking = novoRanking(hoje));
    else if (estado.rankingAnt && estado.rankingAnt.dia === dia) r = estado.rankingAnt;
    else continue;
    somarNoRanking(env, r, chave, topico, g.w, porDia.size === 1 ? linhas : agregarGrupo(env, { ...g, itens }));
  }
}

function somarNoRanking(env, r, chave, topico, w, linhas) {
  const minimo = rankingMinimo(env);
  for (const a of linhas) {
    if (!(a.valor >= minimo) || !(a.valor > 0)) continue;
    if (a.lado === "SELL") { r.vd = r4((r.vd || 0) + a.valor); r.mudou = true; continue; }
    const p = r.partidas[chave] || (r.partidas[chave] = { n: "", v: 0, c: 0, w: {} });
    p.n = truncar(topico.nome || "Partida", 80);
    p.v = r4(p.v + a.valor); p.c += 1; p.w[w] = 1;
    const c = r.carteiras[w] || (r.carteiras[w] = { v: 0, c: 0 });
    c.v = r4(c.v + a.valor); c.c += 1;
    const k = `${w}|${chave}|${a.chave}`;
    const q = r.pos[k] || (r.pos[k] = { w, r: a.rotulo, e: a.escolha, v: 0, s: 0, m: "" });
    q.v = r4(q.v + a.valor); q.s = r4(q.s + a.size); q.m = p.n;
    r.mudou = true;
  }
  podarMapa(r.partidas, MAX_RANKING_PARTIDAS);
  podarMapa(r.pos, MAX_RANKING_POSICOES);
}

const rotuloDia = iso => {
  const d = new Date(`${iso}T12:00:00Z`);
  if (!Number.isFinite(d.getTime())) return diaComAnoBR(iso);
  const sem = new Intl.DateTimeFormat("pt-BR", { timeZone: "UTC", weekday: "short" }).format(d).replace(".", "");
  return `${sem} ${iso.slice(8, 10)}/${iso.slice(5, 7)}`;
};
const plural = (n, um, varios) => `${n} ${n === 1 ? um : varios}`;

function textoRanking(env, r, cfg) {
  const fogo = num(env, "LIMITE_FOGO", 1000);
  const partidas = Object.values(r.partidas || {}).sort((a, b) => b.v - a.v);
  const linhasPartidas = partidas.slice(0, 10).map((p, i) =>
    `${i + 1}. <b>${esc(truncar(p.n, 60))}</b> · US$ ${fmtUsd(p.v)} · ${plural(p.c, "aposta", "apostas")} · 👤 ${Object.keys(p.w || {}).length}`);
  const posicoes = Object.values(r.pos || {}).sort((a, b) => b.v - a.v).slice(0, 5).map(q => {
    const preco = q.s > 0 ? q.v / q.s : 0;
    return `${q.v >= fogo ? "🔥" : "🟢"} <b>${esc(truncar(nomeCarteira(cfg, q.w), 20))}</b> · ${esc(truncar(q.r, 40))}: <b>${esc(truncar(q.e, 40))}</b> · US$ ${fmtUsd(q.v)} @ ${fmtPreco(env, preco)}\n` +
      `<i>↳ ${esc(truncar(q.m, 60))}</i>`;
  });
  const carteiras = Object.entries(r.carteiras || {}).sort((a, b) => b[1].v - a[1].v).slice(0, 5).map(([w, c], i) =>
    `${i + 1}. <b>${esc(truncar(nomeCarteira(cfg, w), 20))}</b> · US$ ${fmtUsd(c.v)} · ${plural(c.c, "aposta", "apostas")}`);
  const total = partidas.reduce((a, p) => a + p.v, 0);
  const apostas = partidas.reduce((a, p) => a + p.c, 0);
  const quantas = Object.keys(r.carteiras || {}).length;
  const secao = (titulo, linhas, limite, resto) =>
    linhas.length ? `${titulo}\n${juntarBlocos(linhas, "\n", limite, resto)}` : "";
  const partes = [
    secao("🎮 <b>Partidas com mais dinheiro</b>", linhasPartidas, 1600, n => `<i>… e mais ${n}</i>`),
    secao("💎 <b>Maiores apostas</b>", posicoes, 1100, n => `<i>… e mais ${n}</i>`),
    secao("👤 <b>Carteiras mais ativas</b>", carteiras, 600, n => `<i>… e mais ${n}</i>`),
  ].filter(Boolean);
  const vendas = r.vd > 0 ? ` · 🔴 vendeu US$ ${fmtUsd(r.vd)}` : "";
  return `🏆 <b>APOSTAS · ${rotuloDia(r.dia)}</b>\n\n` +
    `${partes.join("\n\n") || "<i>aguardando apostas…</i>"}\n\n` +
    `💰 <b>US$ ${fmtUsd(total)}</b> · ${plural(apostas, "aposta", "apostas")} · 👤 ${quantas}${vendas}\n` +
    `<i>desde 00h · mín. US$ ${numeroBR(rankingMinimo(env), 0)} · atualizado ${horaBR(Date.now())} · ${VERSAO_CURTA}</i>`;
}

async function garantirTopicoRanking(env, estado) {
  const fixo = idTelegram(env, env.TELEGRAM_TOPICO_RANKING);
  if (fixo) return fixo;
  const salvo = idTelegram(env, estado.rankingTopico);
  if (salvo) return salvo;
  const c = await criarTopico(env, NOME_TOPICO_RANKING);
  if (!c.ok) { console.warn("ranking: " + limpar(env, c.desc).slice(0, 160)); return 0; }
  estado.rankingTopico = c.id;
  return c.id;
}

// Uma mensagem por dia, editada a cada mudança (no máximo uma edição por RANKING_INTERVALO_S).
async function atualizarRanking(env, estado, cfg, r) {
  if (!r || !r.mudou) return false;
  const intervalo = num(env, "RANKING_INTERVALO_S", PADRAO_RANKING_INTERVALO_S) * 1000;
  const msgId = idTelegram(env, r.msgId);
  if (msgId && !env.run.dry && Date.now() - Number(CACHE.rankingEdicao[r.dia] || 0) < intervalo) return false;
  const texto = textoRanking(env, r, cfg);
  const base = { chat_id: env.TELEGRAM_CHAT_ID, text: texto, parse_mode: "HTML", disable_web_page_preview: true };
  if (msgId) {
    const e = await enviarComRetentativa(env, "editMessageText", { ...base, message_id: msgId });
    if (e.ok || /not[ _]modified/i.test(e.desc || "")) { r.mudou = false; CACHE.rankingEdicao[r.dia] = Date.now(); return true; }
    if (/not found|can't be edited|message_id_invalid|message to edit/i.test((e.desc || "").toLowerCase())) r.msgId = null;
    else { console.warn("ranking: " + limpar(env, e.desc).slice(0, 160)); return false; }
  }
  let topico = await garantirTopicoRanking(env, estado);
  if (!topico) return false;
  let novo = await enviarComRetentativa(env, "sendMessage", { ...base, message_thread_id: topico });
  if (!novo.ok && novo.status === 400 && /thread|topic/i.test(novo.desc || "") && !idTelegram(env, env.TELEGRAM_TOPICO_RANKING)) {
    estado.rankingTopico = 0; // tópico apagado no Telegram: cria outro
    topico = await garantirTopicoRanking(env, estado);
    if (!topico) return false;
    novo = await enviarComRetentativa(env, "sendMessage", { ...base, message_thread_id: topico });
  }
  if (novo.ok && novo.result) { r.msgId = novo.result.message_id; r.mudou = false; CACHE.rankingEdicao[r.dia] = Date.now(); return true; }
  console.warn("ranking não atualizado: " + limpar(env, novo.desc || novo.status).slice(0, 160));
  return false;
}

// ───────────────────────── configuração e estado ─────────────────────────

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
  delete e.indiceCarteira; // campo de versões anteriores
  // a lista agora vive em lista_carteiras; o ID antigo serve só para não duplicar a mensagem na migração
  const listaLegada = e.lista && typeof e.lista === "object" ? e.lista : null;
  delete e.lista;
  Object.defineProperty(e, "listaLegada", { value: listaLegada, enumerable: false, writable: true });
  e.pendentes = Array.isArray(e.pendentes) ? e.pendentes.filter(it => it && it.n && it.w) : [];
  e.vistos = e.vistos && typeof e.vistos === "object" ? e.vistos : {};
  e.ultimo = e.ultimo && typeof e.ultimo === "object" ? e.ultimo : {};
  e.coletas = e.coletas && typeof e.coletas === "object" ? e.coletas : {};
  e.conhecidas = e.conhecidas && typeof e.conhecidas === "object" ? e.conhecidas : {};
  e.piso = e.piso && typeof e.piso === "object" ? e.piso : {};
  e.resumo = e.resumo && typeof e.resumo === "object" ? e.resumo : null;
  e.eventos = e.eventos && typeof e.eventos === "object" && !Array.isArray(e.eventos) ? e.eventos : {};
  const obj = v => (v && typeof v === "object" && !Array.isArray(v) ? v : null);
  e.ranking = obj(e.ranking);
  e.rankingAnt = obj(e.rankingAnt);
  e.rankingTopico = Number.isInteger(Number(e.rankingTopico)) ? Number(e.rankingTopico) : 0;
  return e;
}

async function garantirTopicos(env, estado) {
  if (!(estado.topicos && typeof estado.topicos === "object" && !Array.isArray(estado.topicos))) {
    const legado = await lerTopicosLegado(env);
    if (legado === null) throw new Error("registro de tópicos ilegível; rodada interrompida para evitar duplicações");
    estado.topicos = legado;
  }
  // tópicos sem registro de uso (migração) ganham carência de DIAS_TOPICO a partir de agora
  for (const t of Object.values(estado.topicos)) if (t && !t.ultimoUso) t.ultimoUso = Date.now();
}

// Retrato do estado para saber se algo mudou de verdade. Ficam de fora só os campos que mudam a cada
// rodada sem importar: o ponto de leitura (ultimo) e o horário do último save (salvoEm).
function semCheckpoint(e) {
  const { ultimo, salvoEm, ...resto } = e;
  return JSON.stringify(resto);
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
  // o piso só importa enquanto a janela de sobreposição ainda alcança o momento da reinclusão
  for (const w of Object.keys(estado.piso)) {
    const u = Number(estado.ultimo[w] || 0);
    if (u && u - SOBREPOSICAO_S > Number(estado.piso[w])) delete estado.piso[w];
  }
  // tópico some por falta de uso (não pela data do jogo): evento longo continua com o mesmo tópico
  const limite = Date.now() - DIAS_TOPICO * 86400000;
  for (const chave of Object.keys(estado.topicos)) {
    const t = estado.topicos[chave];
    if (!t || Number(t.ultimoUso || 0) < limite) delete estado.topicos[chave];
  }
  for (const cond of Object.keys(estado.eventos)) {
    if (!estado.topicos[estado.eventos[cond]]) delete estado.eventos[cond];
  }
}

async function gravar(env, estado) {
  podarEstado(estado);
  estado.salvoEm = Date.now();
  const texto = JSON.stringify(estado);
  env.run.estadoBytes = texto.length;
  await kvPut(env, KV_ESTADO, texto);
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

// ───────────────────────── coleta ─────────────────────────

function carteirasPorCiclo(env) {
  const pedido = Math.floor(num(env, "CARTEIRAS_POR_CICLO", PADRAO_CARTEIRAS_POR_CICLO));
  return Math.max(1, Math.min(pedido, env.run.limite - RESERVA_ENVIO - 8));
}

// Rodízio pelo relógio: não grava nada. A cada minuto a janela anda "n" carteiras e, como n é fixo,
// todas as carteiras entram na janela de tempos em tempos.
function selecionarCarteiras(env, carteiras) {
  const n = carteirasPorCiclo(env);
  if (carteiras.length <= n) return carteiras;
  const inicio = (Math.floor(Date.now() / 60000) * n) % carteiras.length;
  return Array.from({ length: n }, (_, i) => carteiras[(inicio + i) % carteiras.length]);
}

// De onde começar a buscar os trades de uma carteira.
function inicioColeta(estado, w, agora) {
  const ultimo = Number(estado.ultimo[w] || 0);
  if (ultimo) return Math.max(ultimo - SOBREPOSICAO_S, agora - JANELA_MAXIMA_S); // continuidade
  if (estado.conhecidas[w]) return agora - SOBREPOSICAO_S;                       // reincluída: a partir de agora
  return agora - JANELA_INICIAL_S;                                               // carteira nova
}

async function coletarCarteira(env, c, estado, pendPorW) {
  const w = c.w;
  const agora = agoraS();
  const existentes = new Set(Array.isArray(estado.vistos[w]) ? estado.vistos[w] : []);
  for (const k of pendPorW.get(w) || []) existentes.add(k);

  let job = estado.coletas[w];
  const jobInvalido = !job || typeof job !== "object"
    || !Number.isFinite(Number(job.start)) || !Number.isFinite(Number(job.end))
    || agora - Number(job.end) > JOB_MAX_IDADE_S; // coleta parada há muito tempo: recomeça
  if (jobInvalido) {
    if (!estado.conhecidas[w]) estado.conhecidas[w] = agora;
    else if (!estado.ultimo[w]) estado.piso[w] = agora; // reincluída: nada anterior a este momento é aviso
    job = { start: Math.max(1, inicioColeta(estado, w, agora)), end: agora, cursor: null };
    estado.coletas[w] = job;
  }
  const piso = Number(estado.piso[w] || 0);
  let novos = 0;
  let concluido = false;
  try {
    const cursorAntes = job.cursor || null;
    const r = await fetchDados(env, montarUrlTrades(w, job.start, job.end, cursorAntes), { headers: { Accept: "application/json" } });
    if (!r.ok) {
      if (r.status === 429) {
        env.run.limitado429 = true; // as outras carteiras esperam a próxima rodada
      } else if (cursorAntes && r.status >= 400 && r.status < 500) {
        job.cursor = null; // cursor recusado: recomeça a janela (a deduplicação evita avisos repetidos)
      }
      throw new Error(`Data API HTTP ${r.status}${r.status === 429 ? " (limite de chamadas; cursor preservado)" : ""}`);
    }
    const body = await r.json();
    let filaCheia = false;
    for (const bruto of listaDe(body)) {
      const n = normalizar(bruto);
      if (!n || !n.cond || !eEsports(n)) continue;
      if (piso && n.ts < piso) continue;
      const k = chaveTrade(n);
      if (existentes.has(k)) continue;
      if (estado.pendentes.length >= MAX_PENDENTES) { filaCheia = true; break; }
      existentes.add(k);
      estado.pendentes.push({ id: `${w}|${k}`, w, n, tent: 0, nextTryAt: 0 });
      novos++;
    }
    const next = body && body.pagination ? body.pagination.next_cursor : null;
    if (filaCheia) {
      job.cursor = cursorAntes;      // retoma esta mesma página na próxima rodada
    } else if (next && String(next) !== cursorAntes) {
      job.cursor = String(next);     // ainda há páginas: continua na próxima rodada
    } else {
      concluido = true;              // sem próxima página (ou cursor repetido: evita laço infinito)
      delete estado.coletas[w];
      estado.ultimo[w] = job.end;
    }
  } finally {
    if (existentes.size) estado.vistos[w] = [...existentes];
    else delete estado.vistos[w];
    if (!concluido) estado.coletas[w] = job;
  }
  return { novos };
}

async function coletar(env, cfg, estado) {
  const alvos = selecionarCarteiras(env, cfg.carteiras);
  const teto = env.run.limite - RESERVA_ENVIO;
  const pendPorW = new Map();
  for (const it of estado.pendentes) {
    if (!pendPorW.has(it.w)) pendPorW.set(it.w, []);
    pendPorW.get(it.w).push(it.id.slice(it.w.length + 1));
  }
  let novos = 0, erros = 0, puladas = 0;
  const amostras = [];
  await mapLimit(alvos, Math.floor(num(env, "CONCORRENCIA", PADRAO_CONCORRENCIA)), async c => {
    if (Date.now() - env.run.inicio > PRAZO_COLETA_MS || env.run.limitado429 || env.run.sub >= teto) { puladas++; return; }
    try {
      novos += (await coletarCarteira(env, c, estado, pendPorW)).novos;
    } catch (e) {
      erros++;
      const msg = `[${c.w.slice(0, 8)}] ${String((e && e.message) || e).slice(0, 160)}`;
      if (amostras.length < 3) amostras.push(msg);
      console.warn(msg);
    }
  });
  return { novos, erros, puladas, consultadas: alvos.length - puladas, amostras };
}

// Carteira removida: some com a fila, o cursor e a deduplicação.
// Se for reincluída, volta como "reincluída" (ver inicioColeta), sem buscar a última hora de novo.
function podarCarteirasRemovidas(estado, cfg) {
  const ativas = new Set(cfg.carteiras.map(c => c.w));
  estado.pendentes = estado.pendentes.filter(it => ativas.has(it.w));
  for (const mapa of [estado.coletas, estado.vistos, estado.ultimo, estado.piso]) {
    for (const w of Object.keys(mapa)) if (!ativas.has(w)) delete mapa[w];
  }
  for (const w of Object.keys(estado.conhecidas)) {
    if (!ativas.has(w) && agoraS() - Number(estado.conhecidas[w]) > 30 * 86400) delete estado.conhecidas[w];
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

// ───────────────────────── evento (Gamma) ─────────────────────────

// Uma passada só (slug do evento; se falhar, o mercado). Quem chama decide quando tentar de novo.
async function buscarEvento(env, n, cacheEventos) {
  const chaveCache = n.eventSlug || `cond:${n.cond}`;
  if (cacheEventos.has(chaveCache)) return cacheEventos.get(chaveCache);
  let ev = null;
  try {
    if (n.eventSlug) {
      const u = new URL(`${GAMMA_API}/events`);
      u.searchParams.set("slug", n.eventSlug);
      const r = await http(env, "gamma", u.toString(), { headers: { Accept: "application/json" } });
      if (r.ok) {
        const lista = await r.json();
        const arr = Array.isArray(lista) ? lista : (lista && Array.isArray(lista.data) ? lista.data : []);
        if (arr.length) ev = extrairEvento(arr[0], n.cond);
      }
    }
    if (!ev) {
      const u = new URL(`${GAMMA_API}/markets`);
      u.searchParams.set("condition_ids", n.cond);
      const r = await http(env, "gamma", u.toString(), { headers: { Accept: "application/json" } });
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
  cacheEventos.set(chaveCache, ev);
  return ev;
}

// Dados do evento guardados no tópico (cache): o que a mensagem precisa, nada além disso.
function snapshotEvento(ev) {
  if (!ev) return null;
  return { titulo: ev.titulo || "", slug: ev.slug || "", times: ev.times || null, inicioMs: ev.inicioMs || 0, data: ev.data || null };
}

function novoTopico(id, nome, ev) {
  return {
    id, nome,
    inicioMs: (ev && ev.inicioMs) || 0,
    dia: (ev && ev.data) || diaBR(Date.now()),
    ev: snapshotEvento(ev),
    evAtualizadoEm: Date.now(),
    ultimoUso: Date.now(),
    resumo: { msgId: null, mercados: {} },
  };
}

// Procura o tópico do evento. Todos os mercados da mesma partida têm o mesmo evento, então caem no mesmo tópico.
function acharTopico(estado, n, ev) {
  const candidatos = [estado.eventos[n.cond], n.eventSlug, ev && ev.slug, n.cond];
  for (const c of candidatos) if (c && estado.topicos[c]) return c;
  return null;
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

// ───────────────────────── mensagens ─────────────────────────

const r4 = x => Math.round(Number(x) * 1e4) / 1e4;
const fmtUsd = v => numeroBR(v, Math.abs(Number(v) || 0) >= 100 ? 0 : 2);
function fmtPreco(env, p) {
  const c = Number(p || 0) * 100;
  const t = numeroBR(c, c < 10 ? 1 : 0);
  return ligado(env && env.PRECO_EM_CENTAVOS) ? `${t}¢` : `${t}%`;
}

function registrarDesconhecido(env, n) {
  const chave = `${n.titulo}|${n.outcome}`;
  if (!CACHE.desconhecidos.has(chave)) {
    if (CACHE.desconhecidos.size > 300) CACHE.desconhecidos.clear();
    CACHE.desconhecidos.add(chave);
    console.warn(`mercado não classificado: titulo="${truncar(n.titulo, 120)}" outcome="${n.outcome}" slug="${n.slug}"`);
  }
  if (env.run.desconhecidos.length < 5) env.run.desconhecidos.push({ titulo: truncar(n.titulo, 120), outcome: n.outcome, slug: n.slug });
}

// Junta as execuções do grupo por lado (compra/venda) e por aposta (mercado + escolha).
function agregarGrupo(env, g) {
  const mapa = new Map();
  for (const it of g.itens) {
    const d = descreverAposta(it.n);
    if (!d.conhecido) registrarDesconhecido(env, it.n);
    const lado = it.n.side === "SELL" ? "SELL" : "BUY";
    const chave = `${d.rotulo} - ${d.escolha}`;
    const k = `${lado}|${chave}`;
    if (!mapa.has(k)) mapa.set(k, { lado, chave, rotulo: d.rotulo, escolha: d.escolha, ordem: d.ordem, valor: 0, size: 0, conhecido: d.conhecido, titulo: it.n.titulo });
    const a = mapa.get(k);
    a.valor += Number(it.n.size || 0) * Number(it.n.price || 0);
    a.size += Number(it.n.size || 0);
  }
  return [...mapa.values()].sort((a, b) => (a.lado === b.lado ? 0 : a.lado === "BUY" ? -1 : 1) || a.ordem - b.ordem);
}

// Uma linha por aposta:  🟢 Carteira · Mapa 4: FURIA · US$ 791 @ 59% · Σ 1.277
//   🔥 a partir de LIMITE_FOGO · 🔴 venda · Σ = quanto a carteira já pôs nessa aposta (só aparece quando soma a uma anterior)
function textoGrupo(env, g, cfg, topico, linhas) {
  const nome = nomeCarteira(cfg, g.w);
  const quem = `<b><a href="https://polymarket.com/profile/${encodeURIComponent(g.w)}">${esc(nome)}</a></b>`;
  const mercados = (topico && topico.resumo && topico.resumo.mercados) || {};
  const fogo = num(env, "LIMITE_FOGO", 1000);
  const textos = linhas.map(a => {
    const preco = a.size > 0 ? a.valor / a.size : 0;
    const dica = a.conhecido === false ? ` · <i>«${esc(truncar(a.titulo, 80))}»</i>` : "";
    const aposta = `${esc(a.rotulo)}: <b>${esc(a.escolha)}</b> · US$ ${fmtUsd(a.valor)} @ ${fmtPreco(env, preco)}${dica}`;
    if (a.lado === "SELL") return `🔴 ${quem} vendeu · ${aposta}`;
    const antes = Number(mercados[a.chave] && mercados[a.chave].w && mercados[a.chave].w[g.w]) || 0;
    const sigma = antes > 0 ? ` · Σ ${fmtUsd(antes + a.valor)}` : "";
    return `${a.valor >= fogo ? "🔥" : "🟢"} ${quem} · ${aposta}${sigma}`;
  });
  let texto = juntarBlocos(textos, "\n", LIMITE_TEXTO, n => `<i>… e mais ${n} aposta(s)</i>`);
  if (ligado(env.MOSTRAR_VERSAO)) texto += ` <i>${VERSAO_CURTA}</i>`;
  return texto;
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

// O placar guarda, por aposta (mercado + escolha): comprado, cotas, vendido, nº de apostas e quanto cada carteira pôs.
function registrarNoResumo(topico, g, linhas) {
  if (!topico.resumo) topico.resumo = { msgId: null, mercados: {} };
  const merc = topico.resumo.mercados || (topico.resumo.mercados = {});
  for (const a of linhas) {
    let m = merc[a.chave];
    if (!m) m = merc[a.chave] = { r: a.rotulo, e: a.escolha, o: a.ordem, v: 0, s: 0, vd: 0, n: 0, w: {} };
    if (m.v === undefined) { // entrada do formato antigo {valor, qtd}
      m.v = Number(m.valor || 0); m.n = Number(m.qtd || 0); m.s = 0; m.vd = 0; m.w = {};
      delete m.valor; delete m.qtd;
    }
    m.r = a.rotulo; m.e = a.escolha; m.o = a.ordem;
    if (a.lado === "SELL") {
      m.vd = r4(m.vd + a.valor);
    } else {
      m.v = r4(m.v + a.valor);
      m.s = r4(m.s + a.size);
      m.n += 1;
      m.w[g.w] = r4((m.w[g.w] || 0) + a.valor);
    }
  }
}

function entradasPlacar(topico) {
  const out = [];
  for (const [chave, m] of Object.entries((topico.resumo && topico.resumo.mercados) || {})) {
    if (!m) continue;
    if (m.v !== undefined) {
      out.push({ r: m.r, e: m.e, o: Number(m.o ?? 200), v: m.v, s: m.s || 0, vd: m.vd || 0, n: m.n || 0, w: m.w || {} });
    } else {
      const i = chave.indexOf(" - ");
      out.push({ r: i > 0 ? chave.slice(0, i) : chave, e: i > 0 ? chave.slice(i + 3) : "", o: 200, v: Number(m.valor || 0), s: 0, vd: 0, n: Number(m.qtd || 0), w: {} });
    }
  }
  return out;
}

function textoPlacar(env, topico, cfg) {
  const entradas = entradasPlacar(topico);
  const grupos = new Map();
  let total = 0, apostas = 0;
  const carteiras = new Set();
  for (const m of entradas) {
    total += m.v; apostas += m.n;
    for (const w of Object.keys(m.w)) carteiras.add(w);
    if (!grupos.has(m.r)) grupos.set(m.r, { ordem: m.o, itens: [] });
    const gr = grupos.get(m.r);
    gr.ordem = Math.min(gr.ordem, m.o);
    gr.itens.push(m);
  }
  const blocos = [...grupos.entries()].sort((a, b) => a[1].ordem - b[1].ordem || a[0].localeCompare(b[0])).map(([rotulo, gr]) => {
    const linhas = gr.itens.sort((a, b) => b.v - a.v).map(m => {
      const partes = [`▸ <b>${esc(m.e)}</b>`];
      if (m.v > 0) partes.push(`US$ ${fmtUsd(m.v)}`);
      if (m.v > 0 && m.s > 0) partes.push(fmtPreco(env, m.v / m.s));
      if (m.vd > 0) partes.push(`vendeu US$ ${fmtUsd(m.vd)}`);
      const quem = Object.entries(m.w).sort((a, b) => b[1] - a[1]).map(([w]) => esc(truncar(nomeCarteira(cfg, w), 12)));
      if (quem.length) partes.push(quem.slice(0, 4).join(", ") + (quem.length > 4 ? ` +${quem.length - 4}` : ""));
      return partes.join(" · ");
    });
    return `<b>${esc(rotulo)}</b>\n${linhas.join("\n")}`;
  });
  const corpo = juntarBlocos(blocos, "\n\n", LIMITE_TEXTO - 500, n => `<i>… e mais ${n} mercado(s)</i>`);
  return `📊 <b>${esc(topico.nome || "Partida")}</b>\n\n` +
    `${corpo || "<i>aguardando apostas…</i>"}\n\n` +
    `💰 <b>US$ ${fmtUsd(total)}</b> · ${apostas} aposta${apostas === 1 ? "" : "s"} · 👤 ${carteiras.size}\n` +
    `<i>atualizado ${horaBR(Date.now())} · ${VERSAO_CURTA}</i>`;
}

// Tópico novo: o placar é a primeira mensagem, para ficar no topo; os avisos vêm abaixo.
async function abrirPlacar(env, topico, cfg) {
  const r = await enviarComRetentativa(env, "sendMessage", {
    chat_id: env.TELEGRAM_CHAT_ID, message_thread_id: topico.id, text: textoPlacar(env, topico, cfg),
    parse_mode: "HTML", disable_web_page_preview: true,
  });
  if (r.ok && r.result) topico.resumo.msgId = r.result.message_id;
}

async function atualizarPlacarVivo(env, topico, cfg) {
  const texto = textoPlacar(env, topico, cfg);
  const msgId = idTelegram(env, topico.resumo.msgId);
  if (msgId) {
    const edit = await enviarComRetentativa(env, "editMessageText", {
      chat_id: env.TELEGRAM_CHAT_ID, message_id: msgId, text: texto,
      parse_mode: "HTML", disable_web_page_preview: true,
    });
    if (edit.ok || /not[ _]modified/i.test(edit.desc || "")) return;
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

// ───────────────────────── envio ─────────────────────────

const RE_TOPICO_SUMIU = /thread not found|TOPIC_ID_INVALID|TOPIC_DELETED/i;

// Acha o tópico do evento. Devolve a chave mesmo quando o ID guardado não vale mais
// (ex.: sobrou de um teste em modo simulado), para que o novo tópico ocupe o mesmo lugar.
function localizar(env, estado, n, ev) {
  const chave = acharTopico(estado, n, ev);
  const topico = chave ? estado.topicos[chave] : null;
  return { chave, topico: topico && idTelegram(env, topico.id) ? topico : null };
}

async function criarTopico(env, nome) {
  const r = await enviarComRetentativa(env, "createForumTopic", { chat_id: env.TELEGRAM_CHAT_ID, name: nome });
  if (!r.ok || !r.result || !r.result.message_thread_id) {
    return { ok: false, status: r.status || 0, desc: "falha ao criar tópico: " + (r.desc || ""), retryAfter: r.retryAfter || 0 };
  }
  return { ok: true, id: r.result.message_thread_id };
}

async function enviarGrupo(env, g, cfg, estado, cacheEventos) {
  const n = g.itens[0].n;
  const tent = Math.max(...g.itens.map(it => Number(it.tent || 0)));
  const linhas = agregarGrupo(env, g);

  let { chave, topico } = localizar(env, estado, n, null);
  let ev = topico && topico.ev ? topico.ev : null;

  // Os dados do evento ficam no tópico: a Gamma só é consultada quando o prazo vence.
  if (!topico || Date.now() - Number(topico.evAtualizadoEm || 0) > (ev ? EV_VALIDADE_MS : EV_FALHA_MS)) {
    const buscado = await buscarEvento(env, n, cacheEventos);
    if (buscado) ev = snapshotEvento(buscado);
    if (topico) {
      topico.evAtualizadoEm = Date.now();
      if (buscado) topico.ev = ev;
    } else {
      if (!buscado && tent < TENTATIVAS_METADADOS) {
        return { ok: false, deferido: true, desc: `aguardando dados do evento [${tent + 1}/${TENTATIVAS_METADADOS}]`, retryAfter: RETRY_METADADOS_S };
      }
      // com o slug da Gamma pode aparecer um tópico que a negociação não indicava
      ({ chave, topico } = localizar(env, estado, n, ev));
    }
  }

  if (!topico) {
    chave = chave || n.eventSlug || (ev && ev.slug) || n.cond;
    const nome = nomeTopicoPartida(ev, n);
    const c = await criarTopico(env, nome);
    if (!c.ok) return c;
    topico = novoTopico(c.id, nome, ev);
    estado.topicos[chave] = topico;
    await abrirPlacar(env, topico, cfg);
  }
  estado.eventos[n.cond] = chave;

  const corpo = {
    chat_id: env.TELEGRAM_CHAT_ID, message_thread_id: topico.id,
    text: textoGrupo(env, g, cfg, topico, linhas), parse_mode: "HTML", disable_web_page_preview: true,
  };
  let r = await enviarTexto(env, corpo);
  if (!r.ok && r.status === 400 && RE_TOPICO_SUMIU.test(r.desc || "")) {
    // tópico apagado no Telegram: cria outro e reenvia
    const nome = nomeTopicoPartida(ev, n);
    const c = await criarTopico(env, nome);
    if (!c.ok) return c;
    topico = novoTopico(c.id, nome, ev);
    estado.topicos[chave] = topico;
    await abrirPlacar(env, topico, cfg);
    r = await enviarTexto(env, { ...corpo, message_thread_id: topico.id, text: textoGrupo(env, g, cfg, topico, linhas) });
  }
  if (r.ok && r.result) {
    topico.ultimoUso = Date.now();
    registrarNoResumo(topico, g, linhas);
    if (rankingLigado(env)) registrarNoRanking(env, estado, chave, topico, g, linhas);
    await atualizarPlacarVivo(env, topico, cfg);
    return { ok: true };
  }
  return { ok: false, status: r.status || 0, desc: r.desc || `HTTP ${r.status}`, retryAfter: r.retryAfter || 0 };
}

// Avisos que já saíram nesta instância mas cuja gravação posterior falhou: não saem de novo.
function esquecerEnviados(estado) {
  const limite = Date.now() - 3 * 3600 * 1000;
  for (const [id, ts] of CACHE.recentes) if (ts < limite) CACHE.recentes.delete(id);
  if (CACHE.recentes.size) estado.pendentes = estado.pendentes.filter(it => !CACHE.recentes.has(it.id));
}

// ───────────────────────── rodada ─────────────────────────

async function ciclo(env0, origem, opcoes = {}) {
  const env = criarCtx(env0, origem, opcoes);
  const run = env.run;
  const resultado = {
    versao: VERSAO, origem, simulado: run.dry, carteiras: 0, consultadas: 0, puladas: 0, novos: 0,
    enviados: 0, falhas: 0, descartados: 0, adiados: 0, pendentes: 0, erros: [],
  };
  const faltam = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "CFG"].filter(k => !env[k]);
  if (faltam.length) {
    resultado.erros.push("faltam configurações: " + faltam.join(", "));
    return finalizar(env, resultado);
  }
  try {
    const cfg = await lerConfig(env);
    resultado.carteiras = cfg.carteiras.length;
    const estado = await lerEstado(env);
    const base = semCheckpoint(estado); // retrato antes de qualquer mudança desta rodada
    await garantirTopicos(env, estado);
    esquecerEnviados(estado);
    rolarRanking(estado);

    // Sem carteiras na configuração (ex.: chave apagada por engano): não mexe em nada.
    if (cfg.carteiras.length) {
      podarCarteirasRemovidas(estado, cfg);
      sincronizarResumo(estado, cfg);
    }
    try {
      await atualizarListaCarteiras(env, cfg, false, estado.listaLegada);
    } catch (e) {
      resultado.erros.push("lista de carteiras: " + truncar(limpar(env, e), 160));
    }

    if (cfg.carteiras.length && estado.pendentes.length < MAX_PENDENTES) {
      const coleta = await coletar(env, cfg, estado);
      resultado.novos = coleta.novos;
      resultado.consultadas = coleta.consultadas;
      resultado.puladas = coleta.puladas;
      if (coleta.erros) resultado.erros.push(`${coleta.erros} carteira(s) não consultadas: ${coleta.amostras.join(" | ")}`);
    }

    // Grava ANTES de enviar. Se a gravação falhar (ex.: cota do dia esgotada), nada é enviado.
    const prontos = agrupar(estado.pendentes).slice(0, MAX_AVISOS);
    const cabeEnvio = () => run.sub + CUSTO_GRUPO + 2 <= run.limite;
    let persistido = base; // o que está no KV, segundo o nosso retrato
    if ((prontos.length && cabeEnvio()) || precisaGravar(estado, base)) {
      await gravar(env, estado);
      persistido = semCheckpoint(estado);
    }

    const cacheEventos = new Map();
    const enviados = new Set();
    const descartados = new Set();
    let processados = 0;
    let parar = false;
    for (const g of prontos) {
      if (parar || Date.now() - run.inicio > PRAZO_ENVIO_MS || !cabeEnvio()) break; // o resto fica na fila
      processados++;
      let r;
      try {
        r = await enviarGrupo(env, g, cfg, estado, cacheEventos);
      } catch (e) {
        r = { ok: false, status: 0, desc: truncar(limpar(env, e), 200) };
      }
      if (r.ok) {
        const agoraMs = Date.now();
        for (const it of g.itens) {
          enviados.add(it.id);
          if (!run.dry) CACHE.recentes.set(it.id, agoraMs);
          if (it.n.ts) run.latencias.push(Math.max(0, Math.round(agoraMs / 1000 - it.n.ts)));
        }
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
      if (r.status === 429) parar = true; // limite do Telegram: o resto espera a próxima rodada
      const motivo = truncar(r.desc || `falha no Telegram (${r.status})`, 160);
      const tipo = classificarFalha(r);
      if (tipo === "permanente" || (tipo === "transitoria" && tentativa >= MAX_TENTATIVAS)) {
        for (const it of g.itens) descartados.add(it.id);
        const alvo = `${nomeCarteira(cfg, g.w)} · ${truncar(g.itens[0].n.titulo, 60)}`;
        console.warn(`aviso descartado (${tipo}, ${tentativa} tentativa(s)): ${alvo} :: ${motivo}`);
        resultado.erros.push(`descartado após ${tentativa} tentativa(s): ${motivo}`);
      } else if (tipo === "config") {
        // token, chat ou permissão: espera e mantém na fila (descartar perderia o aviso por erro do operador)
        for (const it of g.itens) it.nextTryAt = Date.now() + RETRY_CONFIG_S * 1000;
        resultado.erros.push(`configuração do Telegram (nova tentativa em ${RETRY_CONFIG_S}s): ${motivo}`);
      } else {
        const atraso = Math.max(r.retryAfter || 0, Math.min(600, 5 * 2 ** Math.min(tentativa, 7)));
        for (const it of g.itens) {
          it.tent = tentativa;
          it.nextTryAt = Date.now() + atraso * 1000;
        }
        resultado.erros.push(`nova tentativa em ${atraso}s: ${motivo}`);
      }
    }
    resultado.adiados = prontos.length - processados;

    if (enviados.size || descartados.size) {
      estado.pendentes = estado.pendentes.filter(it => !enviados.has(it.id) && !descartados.has(it.id));
    }
    resultado.descartados = descartados.size;

    if (estado.resumo && estado.resumo.mudou && run.sub + 3 <= run.limite) {
      try {
        await atualizarResumo(env, estado, cfg);
      } catch (e) {
        resultado.erros.push("resumo: " + truncar(limpar(env, e), 160));
      }
    }
    if (rankingLigado(env)) {
      for (const r of [estado.ranking, estado.rankingAnt]) {
        if (!r || !r.mudou || run.sub + 4 > run.limite) continue;
        try {
          await atualizarRanking(env, estado, cfg, r);
        } catch (e) {
          resultado.erros.push("ranking: " + truncar(limpar(env, e), 160));
        }
      }
    }
    resultado.pendentes = estado.pendentes.length;

    // Segunda gravação: só se o estado mudou desde a primeira (fila, tópicos, placares, tentativas).
    if (semCheckpoint(estado) !== persistido) await gravarComNovaTentativa(env, estado);
  } catch (e) {
    resultado.erros.push(truncar(limpar(env, e), 300));
  }
  return finalizar(env, resultado);
}

// Fecha a rodada: duração e contadores. O cron grava uma linha no log; o /notify devolve na resposta.
function finalizar(env, resultado) {
  const run = env.run;
  resultado.duracaoMs = Date.now() - run.inicio;
  resultado.requisicoes = run.sub;
  resultado.kvLeitura = run.kvLeitura;
  resultado.kvGravacao = run.kvGravacao;
  resultado.polymarket = run.polymarket;
  resultado.gamma = run.gamma;
  resultado.telegram = run.telegram;
  resultado.estadoBytes = run.estadoBytes;
  if (run.latencias.length) {
    resultado.latenciaMediaS = Math.round(run.latencias.reduce((a, b) => a + b, 0) / run.latencias.length);
    resultado.latenciaMaxS = Math.max(...run.latencias);
  }
  if (run.desconhecidos.length) resultado.mercadosNaoClassificados = run.desconhecidos;
  if (run.simular) resultado.simuladas = run.simuladas;
  if (run.origem === "cron") console.log(JSON.stringify(resultado));
  return resultado;
}

// ───────────────────────── HTTP ─────────────────────────

async function salvarCarteirasPayload(env, body) {
  return salvarCarteiras(env, body && body.carteiras, body && body.base);
}

// Depois de mudar a lista: atualiza só as mensagens da lista (a fila não é tocada).
async function atualizarListaAposConfig(env) {
  try {
    const ctx = criarCtx(env, "config");
    const cfg = await lerConfig(ctx);
    await atualizarListaCarteiras(ctx, cfg, true);
  } catch (e) {
    console.warn("falha ao atualizar a lista de carteiras: " + limpar(env, e));
  }
}

function igual(a, b) {
  const x = String(a), y = String(b);
  let d = x.length ^ y.length;
  for (let i = 0; i < Math.max(x.length, y.length); i++) d |= (x.charCodeAt(i) || 0) ^ (y.charCodeAt(i) || 0);
  return d === 0;
}
function autorizado(req, env, body) {
  if (!env.CHAVE) return false;
  const auth = req.headers.get("Authorization") || "";
  const token = auth.replace(/^Bearer\s+/i, "").trim() || (body && typeof body.chave === "string" ? body.chave : "");
  return igual(token, env.CHAVE);
}

export default {
  async fetch(req, env) {
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method === "GET" && path === "/") return textResp(`ok · ${VERSAO}`);

    // Para monitor externo: 503 se o estado não é gravado há mais de STATUS_MAX_IDADE_MIN minutos
    // (cron parado ou cota de gravações esgotada). Não grava nada.
    if (req.method === "GET" && path === "/status") {
      try {
        const ctx = criarCtx(env, "status");
        const estado = await lerEstado(ctx);
        const cfg = await lerConfig(ctx);
        const idade = estado.salvoEm ? Math.round((Date.now() - Number(estado.salvoEm)) / 60000) : null;
        const ok = idade !== null && idade <= STATUS_MAX_IDADE_MIN;
        return jsonResp({
          ok, versao: VERSAO, carteiras: cfg.carteiras.length, pendentes: estado.pendentes.length,
          topicos: Object.keys(estado.topicos || {}).length, ultimaGravacaoHaMin: idade,
          aviso: ok ? undefined : `estado sem gravação há mais de ${STATUS_MAX_IDADE_MIN} min`,
        }, ok ? 200 : 503);
      } catch (e) {
        return jsonResp({ ok: false, versao: VERSAO, erro: truncar(limpar(env, e), 200) }, 503);
      }
    }

    if (req.method === "GET" && path === "/config") {
      if (!autorizado(req, env, null)) return jsonResp({ ok: false, erro: "não autorizado" }, 401);
      try {
        const cfg = await lerConfig(criarCtx(env, "config"));
        return jsonResp({ ok: true, carteiras: cfg.carteiras, atualizado: cfg.atualizado, versao: VERSAO });
      } catch (e) {
        return jsonResp({ ok: false, erro: truncar(limpar(env, e), 300) }, 400);
      }
    }

    if (req.method !== "POST" || (path !== "/config" && path !== "/notify")) return textResp("não encontrado", 404);

    let body = null;
    try { body = await req.json(); } catch (_) { return jsonResp({ ok: false, erro: "corpo JSON inválido" }, 400); }
    if (!autorizado(req, env, body)) return jsonResp({ ok: false, erro: "não autorizado" }, 401);

    try {
      const ctx = criarCtx(env, "http");
      if (path === "/config") {
        const salvos = await salvarCarteirasPayload(ctx, body);
        await atualizarListaAposConfig(env);
        return jsonResp({ ok: true, ...salvos, versao: VERSAO });
      }

      if (body && (body.titulo || body.corpo)) {
        const topicoId = await garantirTopicoAtualizacoes(ctx);
        const msg = { chat_id: env.TELEGRAM_CHAT_ID, text: `<b>${esc(truncar(body.titulo || "Aviso", 200))}</b>\n${esc(truncar(body.corpo || "", 3000))}`, parse_mode: "HTML", disable_web_page_preview: true };
        if (topicoId) msg.message_thread_id = topicoId;
        const r = await enviarComRetentativa(ctx, "sendMessage", msg);
        if (!r.ok) return jsonResp({ ok: false, erro: truncar(limpar(env, r.desc || r.status), 300), versao: VERSAO }, 502);
        return jsonResp({ ok: true, enviado: true, versao: VERSAO });
      }

      const out = { ok: true, versao: VERSAO };
      if (body && Array.isArray(body.carteiras)) {
        Object.assign(out, await salvarCarteirasPayload(ctx, body));
        await atualizarListaAposConfig(env);
      }
      const simular = (body && body.simular === true) || url.searchParams.get("simular") === "1";
      out.ciclo = await ciclo(env, "manual", { simular });
      return jsonResp(out);
    } catch (e) {
      if (e && e.conflito) {
        const atual = await lerConfig(criarCtx(env, "config")).catch(() => ({ carteiras: [], atualizado: null }));
        return jsonResp({ ok: false, conflito: true, erro: "lista alterada em outro aparelho", carteiras: atual.carteiras, atualizado: atual.atualizado, versao: VERSAO }, 409);
      }
      return jsonResp({ ok: false, erro: truncar(limpar(env, e), 300), versao: VERSAO }, 400);
    }
  },

  async scheduled(_event, env) {
    await ciclo(env, "cron");
  },
};

