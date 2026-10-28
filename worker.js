/**
 * CS2 Wallet Scanner v55 — Match Winner resolvido (candidato a validação)
 *
 * Rotas:
 *   GET  /health
 *   GET  /api/scan?hours=48&days=30&limit=40
 *   GET  /api/wallet?address=0x...&days=30&limit=100
 *   POST /config  (Authorization: Bearer AUTH_TOKEN; JSON {carteiras:[{w,nome}]})
 *   POST /notify  (Authorization: Bearer AUTH_TOKEN; JSON {text})
 *
 * Bindings/vars opcionais:
 *   SCANNER_KV (KV namespace)
 *   AUTH_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
 *   MAX_MARKETS (default 40), TRADE_MAX_PAGES (default 10)
 *
 * ATENÇÃO: candidato a validação; não publicar sem testar em ambiente controlado.
 */

const GAMMA = "https://gamma-api.polymarket.com";
const DATA_API = "https://data-api.polymarket.com/v2";
const DEFAULT_MARKETS = 40;
const DEFAULT_TRADE_PAGES = 5;
const DISCOVERY_PAGE_SIZE = 100;
const MAX_DISCOVERY_PAGES = 5;
const KV_CONFIG_KEY = "config:wallets";
const KV_LATEST_KEY = "scan:latest";

function json(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      ...extraHeaders
    }
  });
}

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "Content-Type, Authorization"
  };
}

async function apiJSON(url) {
  const response = await fetch(url, {
    headers: { accept: "application/json", "user-agent": "CS2-Wallet-Scanner/55" }
  });
  if (!response.ok) {
    let detail = "";
    try {
      const body = await response.text();
      detail = body ? `: ${body.slice(0, 250)}` : "";
    } catch (_) {}
    throw new Error(`HTTP ${response.status} ${response.statusText}${detail}`);
  }
  return response.json();
}

function parseMaybeJSON(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

function marketText(market) {
  const listText = value => {
    if (Array.isArray(value)) return value.map(item => {
      if (typeof item === "string") return item;
      if (!item || typeof item !== "object") return "";
      return [item.label, item.slug, item.title, item.name, item.ticker, item.description].filter(Boolean).join(" ");
    }).join(" ");
    if (typeof value === "string") {
      try { const parsed = JSON.parse(value); if (Array.isArray(parsed)) return listText(parsed); } catch (_) {}
      return value;
    }
    return "";
  };
  const events = Array.isArray(market.events) ? market.events.map(e => [
    e?.title, e?.slug, e?.ticker, e?.description, e?.seriesSlug, e?.series_slug,
    e?.category, e?.subcategory, e?.sportsMarketType, e?.eventSlug
  ].filter(Boolean).join(" ")).join(" ") : "";
  return [
    market.question, market.title, market.slug, market.description,
    market.eventSlug, market.event_slug, market.groupItemTitle, market.group_item_title,
    market.ticker, market.category, market.subcategory, market.sportsMarketType,
    market.sports_market_type, market.seriesSlug, market.series_slug,
    market.gameStartTime, market.game_start_time, market.competitive,
    listText(market.tags), listText(market.events), events
  ].filter(Boolean).join(" ").toLowerCase();
}

function isCS2Market(market) {
  const text = marketText(market);
  return /\bcs2\b|\bcs:2\b|\bcsgo\b|\bcs:go\b|counter[-\s]?strike\s*2|counterstrike\s*2|counter[-\s]?strike:?\s*global offensive|global offensive/.test(text);
}

function marketTypeText(market) {
  // Classifique o mercado em si. O título do evento pode mencionar mapas/rounds
  // mesmo quando o mercado individual é o vencedor da partida inteira.
  return [market.question, market.title, market.slug, market.groupItemTitle,
    market.group_item_title, market.sportsMarketType, market.sports_market_type,
    market.marketType, market.market_type].filter(Boolean).join(" ").toLowerCase();
}

function isFullMatchWinnerMarket(market) {
  const text = marketTypeText(market);
  // Excluir mercados de escopo parcial/estatístico antes de aceitar palavras como winner.
  const secondary = /\b(map(?:\s*\d+)?|game\s*\d+|rounds?|first\s*kill|first\s*map|halves?|correct\s*score|exact\s*score|handicap|spread|totals?|over\s*\/\s*under|over|under|both\s*teams|race\s*to|pistol|kills?|bombs?|series\s*score|map\s*handicap|round\s*handicap)\b/i;
  if (secondary.test(text)) return false;

  const explicit = /\b(match\s*winner|winner\s*of\s*(the\s*)?(match|series)|moneyline|to\s*win\s*(the\s*)?(match|series)|match\s*result|winner\s*\(match\)|full\s*match\s*winner)\b/i;
  if (explicit.test(text)) return true;

  // Fallback conservador: só usa os campos do próprio mercado, nunca outcomes/evento.
  const hasHeadToHead = /\b(vs\.?|versus)\b/i.test(text);
  const hasWinnerContext = /\b(winner|win|victory|victor)\b/i.test(text);
  return hasHeadToHead && hasWinnerContext;
}

function getCondition(market) {
  return String(market.conditionId || market.condition_id || "").toLowerCase();
}

function parsePrices(market) {
  let value = market.outcomePrices ?? market.outcome_prices ?? market.prices;
  if (typeof value === "string") { try { value = JSON.parse(value); } catch (_) { return []; } }
  if (!Array.isArray(value)) return [];
  return value.map(Number).filter(Number.isFinite);
}

function hasDecisiveResolution(market) {
  // "active" pode continuar true em alguns registros Gamma mesmo com closed=true;
  // portanto, closed + active não deve rejeitar automaticamente o mercado.
  if (market.closed !== true || market.archived === true) return false;

  const prices = parsePrices(market);
  if (prices.length < 2) return false;
  const decisivePrices = Math.max(...prices) >= 0.99 && Math.min(...prices) <= 0.01;
  if (!decisivePrices) return false;

  // Exigir evidência de encerramento/resolução além de preços 0/1.
  const closedTimeRaw = market.closedTime ?? market.closed_time ?? null;
  const closedTime = closedTimeRaw ? Date.parse(String(closedTimeRaw)) : NaN;
  const closedTimeIsPast = Number.isFinite(closedTime) && closedTime <= Date.now();
  const status = String(
    market.umaResolutionStatus ?? market.uma_resolution_status ??
    market.resolutionStatus ?? market.resolution_status ?? ""
  ).toLowerCase().trim();
  const terminalStatus = /^(resolved|finalized|confirmed|settled|resolved_yes|resolved_no|resolved-yes|resolved-no)$/.test(status);

  return closedTimeIsPast || terminalStatus;
}

function marketDate(market) {
  return Date.parse(market.updatedAt || market.updated_at || market.endDate || market.end_date ||
    market.startDate || market.start_date || market.createdAt || market.created_at || "") || 0;
}

function sampleMarket(market) {
  const events = Array.isArray(market.events) ? market.events.slice(0, 2).map(e => ({
    title: e?.title || null, slug: e?.slug || null, ticker: e?.ticker || null,
    closed: e?.closed ?? null, active: e?.active ?? null, endDate: e?.endDate || e?.end_date || null,
    closedTime: e?.closedTime || e?.closed_time || null
  })) : [];
  return {
    question: market.question || null, title: market.title || null, slug: market.slug || null,
    eventSlug: market.eventSlug || market.event_slug || null,
    groupItemTitle: market.groupItemTitle || market.group_item_title || null,
    ticker: market.ticker || null, category: market.category || null,
    sportsMarketType: market.sportsMarketType || market.sports_market_type || null,
    conditionId: market.conditionId || market.condition_id || null,
    active: market.active ?? null, closed: market.closed ?? null, archived: market.archived ?? null,
    outcomePrices: market.outcomePrices || market.outcome_prices || null,
    outcomes: market.outcomes || null,
    closedTime: market.closedTime || market.closed_time || null,
    umaResolutionStatus: market.umaResolutionStatus || market.uma_resolution_status || null,
    resolutionStatus: market.resolutionStatus || market.resolution_status || null,
    acceptingOrders: market.acceptingOrders ?? market.accepting_orders ?? null,
    endDate: market.endDate || market.end_date || null,
    events
  };
}

async function getRecentCS2Markets(maxMarkets, diagnostics) {
  const markets = [];
  const seen = new Set();
  const queries = [
    { name: "closed", active: null, closed: "true" },
    // Coletamos uma amostra de ativos apenas para diagnosticar a descoberta;
    // eles não são elegíveis para a saída final.
    { name: "active", active: "true", closed: "false" }
  ];

  for (const query of queries) {
    let reachedPageCap = true;
    for (let page = 0; page < MAX_DISCOVERY_PAGES; page++) {
      const offset = page * DISCOVERY_PAGE_SIZE;
      const url = new URL(`${GAMMA}/markets`);
      if (query.active !== null) url.searchParams.set("active", query.active);
      url.searchParams.set("closed", query.closed);
      url.searchParams.set("limit", String(DISCOVERY_PAGE_SIZE));
      url.searchParams.set("offset", String(offset));
      url.searchParams.set("order", "id");
      url.searchParams.set("ascending", "false");

      let batch;
      try {
        batch = await apiJSON(url.toString());
      } catch (error) {
        diagnostics.errors.push(`Gamma ${query.name} offset ${offset}: ${error.message}`);
        diagnostics.collectionIncomplete = true;
        reachedPageCap = false;
        break;
      }
      if (!Array.isArray(batch) || batch.length === 0) { reachedPageCap = false; break; }
      if (query.name === "active") diagnostics.activePagesFetched++;
      else diagnostics.closedPagesFetched++;
      diagnostics.apiItems += batch.length;
      if (diagnostics.rawSamples.length < 6) {
        for (const m of batch.slice(0, 6 - diagnostics.rawSamples.length)) diagnostics.rawSamples.push({ query: query.name, ...sampleMarket(m) });
      }

      for (const market of batch) {
        const text = marketText(market);
        if (isCS2Market(market)) {
          diagnostics.classifiedCS2++;
          if (diagnostics.cs2Samples.length < 8) diagnostics.cs2Samples.push(sampleMarket(market));
          if (!isFullMatchWinnerMarket(market)) continue;
          diagnostics.classifiedMatchWinnerOnly++;
          if (diagnostics.matchWinnerSamples.length < 8) diagnostics.matchWinnerSamples.push(sampleMarket(market));
          if (query.name !== "closed" || !hasDecisiveResolution(market)) continue;
          diagnostics.resolvedMatchWinner++;
          const condition = getCondition(market);
          if (!/^0x[a-f0-9]{64}$/.test(condition) || seen.has(condition)) continue;
          seen.add(condition);
          markets.push({ condition, market });
        } else if (diagnostics.cs2LikeSamples.length < 8 && /\b(cs2|cs:go|csgo|counter[-\s]?strike|global offensive)\b/i.test(text)) {
          diagnostics.cs2LikeSamples.push(sampleMarket(market));
        }
      }
      if (batch.length < DISCOVERY_PAGE_SIZE) { reachedPageCap = false; break; }
    }
    if (reachedPageCap) diagnostics.truncated = true;
  }

  markets.sort((a, b) => marketDate(b.market) - marketDate(a.market));
  diagnostics.uniqueResolvedMatchWinnerMarkets = markets.length;
  diagnostics.marketsSelectedForTrades = Math.min(markets.length, maxMarkets);
  return markets.slice(0, maxMarkets);
}

function tradeTimestamp(trade) {
  const raw = trade.timestamp ?? trade.block_timestamp ?? trade.blockTimestamp ?? trade.created_at ?? trade.createdAt;
  if (typeof raw === "number" || /^\d+$/.test(String(raw ?? ""))) {
    const n = Number(raw);
    if (!Number.isFinite(n)) return 0;
    return n > 1e12 ? Math.floor(n / 1000) : n;
  }
  const parsed = Date.parse(String(raw || ""));
  return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : 0;
}

function tradeWallet(trade) {
  const value = String(trade.proxy_wallet || trade.proxyWallet || trade.wallet || trade.user || "").toLowerCase();
  return /^0x[a-f0-9]{40}$/.test(value) ? value : "";
}

function tradeIdentity(trade) {
  // Não usar transaction_hash isoladamente: a mesma transação pode ter vários participantes/linhas.
  return [
    trade.transaction_hash || trade.transactionHash || "",
    trade.proxy_wallet || trade.proxyWallet || trade.wallet || trade.user || "",
    trade.timestamp || trade.block_timestamp || "",
    trade.token_id || trade.asset || trade.tokenId || "",
    trade.side || "",
    trade.size ?? "",
    trade.price ?? ""
  ].join("|").toLowerCase();
}

async function getTradesForCondition(condition, days, maxPages, diagnostics) {
  const trades = [];
  const cutoff = Math.floor(Date.now() / 1000) - days * 86400;
  let cursor = null;
  const seenCursors = new Set();
  const seenTrades = new Set();
  let oldestTimestamp = Infinity;
  let reachedCutoff = false;

  for (let page = 0; page < maxPages; page++) {
    const url = new URL(`${DATA_API}/trades`);
    url.searchParams.set("condition", condition);
    url.searchParams.set("limit", "100");
    url.searchParams.set("taker_only", "false");
    if (cursor) url.searchParams.set("cursor", cursor);

    let response;
    try {
      response = await apiJSON(url.toString());
    } catch (error) {
      diagnostics.errors.push(`Trades ${condition} página ${page + 1}: ${error.message}`);
      diagnostics.tradeErrors++;
      diagnostics.collectionIncomplete = true;
      break;
    }

    const batch = Array.isArray(response) ? response : Array.isArray(response?.data) ? response.data : [];
    diagnostics.tradePagesFetched++;
    diagnostics.tradeRowsReceived += batch.length;

    if (batch.length === 0) break;
    for (const trade of batch) {
      const timestamp = tradeTimestamp(trade);
      if (timestamp > 0) oldestTimestamp = Math.min(oldestTimestamp, timestamp);
      if (timestamp > 0 && timestamp < cutoff) {
        reachedCutoff = true;
        continue;
      }
      if (timestamp <= 0) {
        diagnostics.tradesWithoutTimestamp++;
        continue;
      }
      const key = tradeIdentity(trade);
      if (!seenTrades.has(key)) {
        seenTrades.add(key);
        trades.push({ ...trade, _scannerTimestamp: timestamp, _condition: condition });
      }
    }

    const nextCursor = response?.next_cursor ?? response?.nextCursor ?? response?.pagination?.next_cursor ?? response?.pagination?.nextCursor ?? null;
    const hasMore = response?.has_more ?? response?.hasMore ?? Boolean(nextCursor);
    if (!hasMore || !nextCursor || batch.length === 0 || reachedCutoff) break;
    if (seenCursors.has(nextCursor)) {
      diagnostics.errors.push(`Trades ${condition}: cursor repetido; paginação interrompida.`);
      diagnostics.collectionIncomplete = true;
      break;
    }
    seenCursors.add(nextCursor);
    cursor = nextCursor;
  }
  return trades;
}

async function loadWalletConfig(env) {
  if (!env.SCANNER_KV) return [];
  try {
    const config = await env.SCANNER_KV.get(KV_CONFIG_KEY, "json");
    return Array.isArray(config?.carteiras) ? config.carteiras : [];
  } catch (_) {
    return [];
  }
}

async function runScan(env, params = {}) {
  const hours = Math.max(1, Math.min(720, Number(params.hours) || 48));
  const days = Math.max(1, Math.min(90, Number(params.days) || 30));
  const requestedLimit = Math.max(1, Math.min(200, Number(params.limit) || DEFAULT_MARKETS));
  const envMax = Math.max(1, Math.min(200, Number(env.MAX_MARKETS) || DEFAULT_MARKETS));
  const maxMarkets = Math.min(requestedLimit, envMax);
  const maxPages = Math.max(1, Math.min(20, Number(env.TRADE_MAX_PAGES) || DEFAULT_TRADE_PAGES));

  const diagnostics = {
    generatedAt: new Date().toISOString(),
    activePagesFetched: 0,
    closedPagesFetched: 0,
    apiItems: 0,
    classifiedCS2: 0,
    classifiedMatchWinnerOnly: 0,
    resolvedMatchWinner: 0,
    uniqueResolvedMatchWinnerMarkets: 0,
    marketsSelectedForTrades: 0,
    rawSamples: [],
    cs2LikeSamples: [],
    cs2Samples: [],
    matchWinnerSamples: [],
    truncated: false,
    tradePagesFetched: 0,
    tradeRowsReceived: 0,
    tradesInHistoryWindow: 0,
    tradesInDiscoveryWindow: 0,
    walletsFound: 0,
    tradeErrors: 0,
    tradesWithoutTimestamp: 0,
    collectionIncomplete: false,
    errors: []
  };

  const markets = await getRecentCS2Markets(maxMarkets, diagnostics);
  const historyCutoff = Math.floor(Date.now() / 1000) - days * 86400;
  const discoveryCutoff = Math.floor(Date.now() / 1000) - hours * 3600;
  const allTrades = [];
  const seenTradeKeys = new Set();
  const wallets = new Map();
  const marketSummaries = [];

  for (const entry of markets) {
    const marketTrades = await getTradesForCondition(entry.condition, days, maxPages, diagnostics);
    const inWindow = marketTrades.filter(t => t._scannerTimestamp >= historyCutoff);
    let discoveryCount = 0;
    for (const trade of inWindow) {
      const key = tradeIdentity(trade);
      if (!seenTradeKeys.has(key)) {
        seenTradeKeys.add(key);
        allTrades.push(trade);
      }
      if (trade._scannerTimestamp >= discoveryCutoff) discoveryCount++;
      const wallet = tradeWallet(trade);
      if (wallet) {
        if (!wallets.has(wallet)) wallets.set(wallet, { address: wallet, trades: 0, recentTrades: 0, lastTradeAt: 0, markets: new Set() });
        const item = wallets.get(wallet);
        item.trades++;
        if (trade._scannerTimestamp >= discoveryCutoff) item.recentTrades++;
        item.lastTradeAt = Math.max(item.lastTradeAt, trade._scannerTimestamp);
        item.markets.add(entry.condition);
      }
    }
    diagnostics.tradesInHistoryWindow += inWindow.length;
    diagnostics.tradesInDiscoveryWindow += discoveryCount;
    marketSummaries.push({
      condition: entry.condition,
      question: entry.market.question || entry.market.title || entry.market.slug || "Mercado CS2",
      slug: entry.market.slug || null,
      active: entry.market.active ?? null,
      closed: entry.market.closed ?? null,
      outcomePrices: entry.market.outcomePrices || entry.market.outcome_prices || null,
      closedTime: entry.market.closedTime || entry.market.closed_time || null,
      umaResolutionStatus: entry.market.umaResolutionStatus || entry.market.uma_resolution_status || null,
      resolutionStatus: entry.market.resolutionStatus || entry.market.resolution_status || null,
      resolved: hasDecisiveResolution(entry.market),
      tradesInHistoryWindow: inWindow.length,
      tradesInDiscoveryWindow: discoveryCount
    });
  }

  const walletList = [...wallets.values()].map(w => ({
    address: w.address,
    trades: w.trades,
    recentTrades: w.recentTrades,
    lastTradeAt: w.lastTradeAt ? new Date(w.lastTradeAt * 1000).toISOString() : null,
    marketsCount: w.markets.size
  })).sort((a, b) => b.recentTrades - a.recentTrades || b.trades - a.trades);

  diagnostics.walletsFound = walletList.length;
  const configuredWallets = await loadWalletConfig(env);
  const configuredSet = new Set(configuredWallets.map(w => String(w.w || w.address || "").toLowerCase()));
  for (const wallet of walletList) wallet.configured = configuredSet.has(wallet.address);

  allTrades.sort((a, b) => b._scannerTimestamp - a._scannerTimestamp);
  const result = {
    ok: diagnostics.errors.length === 0,
    service: "cs2-wallet-scanner-v55-match-winner-resolved",
    generatedAt: diagnostics.generatedAt,
    windows: { discoveryHours: hours, historyDays: days },
    marketCount: markets.length,
    configuredWalletCount: configuredWallets.length,
    tradeCount: allTrades.length,
    recentTradeCount: allTrades.filter(t => t._scannerTimestamp >= discoveryCutoff).length,
    walletCount: walletList.length,
    markets: marketSummaries,
    wallets: walletList,
    trades: allTrades.slice(0, 500).map(t => ({
      condition: t._condition,
      proxy_wallet: tradeWallet(t) || null,
      side: t.side || null,
      token_id: t.token_id || t.asset || t.tokenId || null,
      size: t.size ?? null,
      price: t.price ?? null,
      timestamp: t._scannerTimestamp,
      transaction_hash: t.transaction_hash || t.transactionHash || null
    })),
    diagnostics
  };

  if (env.SCANNER_KV) {
    try { await env.SCANNER_KV.put(KV_LATEST_KEY, JSON.stringify(result), { expirationTtl: 86400 }); }
    catch (error) { result.diagnostics.errors.push(`KV save: ${error.message}`); result.diagnostics.collectionIncomplete = true; result.ok = false; }
  }
  return result;
}

async function getWalletTrades(address, days, limit) {
  const normalized = String(address || "").toLowerCase();
  if (!/^0x[a-f0-9]{40}$/.test(normalized)) {
    return { error: "Endereço de carteira inválido; esperado 0x seguido de 40 caracteres hexadecimais." };
  }
  const cutoff = Math.floor(Date.now() / 1000) - days * 86400;
  const trades = [];
  const seen = new Set();
  let cursor = null;
  let pagesFetched = 0;
  const maxPages = Math.max(1, Math.min(20, Math.ceil(limit / 100)));
  const errors = [];

  for (let page = 0; page < maxPages; page++) {
    const url = new URL(`${DATA_API}/trades`);
    url.searchParams.set("user", normalized);
    url.searchParams.set("limit", String(Math.min(100, limit)));
    url.searchParams.set("taker_only", "false");
    if (cursor) url.searchParams.set("cursor", cursor);
    try {
      const response = await apiJSON(url.toString());
      const batch = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
      pagesFetched++;
      for (const trade of batch) {
        const timestamp = tradeTimestamp(trade);
        if (!timestamp || timestamp < cutoff) continue;
        const key = tradeIdentity(trade);
        if (!seen.has(key)) { seen.add(key); trades.push({ ...trade, _scannerTimestamp: timestamp }); }
      }
      const next = response?.pagination?.next_cursor ?? null;
      if (!next || batch.length === 0) break;
      cursor = next;
    } catch (error) {
      errors.push(error.message || String(error));
      break;
    }
  }
  trades.sort((a, b) => b._scannerTimestamp - a._scannerTimestamp);
  return {
    ok: errors.length === 0,
    address: normalized,
    days,
    tradeCount: trades.length,
    pagesFetched,
    errors,
    trades: trades.slice(0, limit).map(t => ({
      proxy_wallet: tradeWallet(t) || normalized,
      condition_id: t.condition_id || t.conditionId || null,
      side: t.side || null,
      token_id: t.token_id || t.asset || t.tokenId || null,
      size: t.size ?? null,
      price: t.price ?? null,
      timestamp: t._scannerTimestamp,
      transaction_hash: t.transaction_hash || t.transactionHash || null,
      title: t.title || null,
      slug: t.slug || null
    }))
  };
}

async function sendTelegram(env, text) {
  if (!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) {
    return { ok: false, error: "Telegram não configurado: defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID." };
  }
  const response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text: String(text || "").slice(0, 4000) })
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok || !body.ok) return { ok: false, status: response.status, error: body.description || "Falha ao enviar Telegram." };
  return { ok: true };
}

function authorized(request, env) {
  if (!env.AUTH_TOKEN) return false;
  return request.headers.get("authorization") === `Bearer ${env.AUTH_TOKEN}`;
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders() });

    try {
      if (request.method === "GET" && path === "/health") {
        return json({
          ok: true,
          service: "cs2-wallet-scanner-v55-match-winner-resolved",
          time: new Date().toISOString(),
          kvConfigured: Boolean(env.SCANNER_KV),
          telegramConfigured: Boolean(env.TELEGRAM_BOT_TOKEN && env.TELEGRAM_CHAT_ID),
          authConfigured: Boolean(env.AUTH_TOKEN)
        }, 200, corsHeaders());
      }

      if (request.method === "GET" && path === "/api/scan") {
        const result = await runScan(env, {
          hours: url.searchParams.get("hours"),
          days: url.searchParams.get("days"),
          limit: url.searchParams.get("limit")
        });
        return json(result, result.ok ? 200 : 502, corsHeaders());
      }

      if (request.method === "GET" && path === "/api/wallet") {
        const result = await getWalletTrades(
          url.searchParams.get("address"),
          Math.max(1, Math.min(90, Number(url.searchParams.get("days")) || 30)),
          Math.max(1, Math.min(1000, Number(url.searchParams.get("limit")) || 100))
        );
        if (result.error) return json({ ok: false, error: result.error }, 400, corsHeaders());
        return json(result, result.ok ? 200 : 502, corsHeaders());
      }

      if (request.method === "POST" && path === "/config") {
        if (!authorized(request, env)) return json({ ok: false, error: "Unauthorized; configure AUTH_TOKEN e envie Authorization: Bearer ..." }, 401, corsHeaders());
        if (!env.SCANNER_KV) return json({ ok: false, error: "Binding SCANNER_KV não configurado." }, 503, corsHeaders());
        const body = await request.json().catch(() => null);
        if (!body || !Array.isArray(body.carteiras)) return json({ ok: false, error: "Body esperado: { carteiras: [{ w, nome }] }" }, 400, corsHeaders());
        const clean = [];
        const seen = new Set();
        for (const item of body.carteiras) {
          const w = String(item?.w || item?.address || "").toLowerCase();
          if (!/^0x[a-f0-9]{40}$/.test(w) || seen.has(w)) continue;
          seen.add(w);
          clean.push({ w, nome: String(item?.nome || item?.name || w).slice(0, 100) });
        }
        await env.SCANNER_KV.put(KV_CONFIG_KEY, JSON.stringify({ carteiras: clean, updatedAt: new Date().toISOString() }));
        return json({ ok: true, saved: clean.length, carteiras: clean }, 200, corsHeaders());
      }

      if (request.method === "POST" && path === "/notify") {
        if (!authorized(request, env)) return json({ ok: false, error: "Unauthorized" }, 401, corsHeaders());
        const body = await request.json().catch(() => ({}));
        const result = await sendTelegram(env, body.text || "CS2 Wallet Scanner test notification");
        return json(result, result.ok ? 200 : 503, corsHeaders());
      }

      return json({ ok: false, error: "Not found", routes: ["GET /health", "GET /api/scan", "GET /api/wallet?address=0x...", "POST /config", "POST /notify"] }, 404, corsHeaders());
    } catch (error) {
      return json({ ok: false, error: error?.message || String(error), service: "cs2-wallet-scanner-v55-match-winner-resolved" }, 500, corsHeaders());
    }
  },

  async scheduled(event, env, ctx) {
    // Cron faz uma coleta limitada; alertas Telegram são opcionais e só avisam novas negociações
    // quando há um resultado anterior no KV para comparação.
    ctx.waitUntil((async () => {
      const result = await runScan(env, { hours: 1, days: 30, limit: DEFAULT_MARKETS });
      if (!result.ok || !env.SCANNER_KV) return;
      const previous = await env.SCANNER_KV.get("scan:last-alert-state", "json").catch(() => null);
      const currentKeys = result.trades
        .filter(t => t.timestamp >= Math.floor(Date.now() / 1000) - 3600)
        .map(t => `${t.transaction_hash || ""}|${t.proxy_wallet || ""}|${t.token_id || ""}|${t.timestamp}`);
      const oldSet = new Set(Array.isArray(previous?.keys) ? previous.keys : []);
      const fresh = currentKeys.filter(k => !oldSet.has(k));
      await env.SCANNER_KV.put("scan:last-alert-state", JSON.stringify({ keys: currentKeys.slice(0, 2000), updatedAt: new Date().toISOString() }), { expirationTtl: 7 * 86400 });
      if (fresh.length && env.TELEGRAM_BOT_TOKEN && env.TELEGRAM_CHAT_ID) {
        await sendTelegram(env, `CS2 Wallet Scanner: ${fresh.length} novo(s) registro(s) de trade detectado(s) na última hora. Carteiras encontradas: ${result.walletCount}.`);
      }
    })());
  }
};
