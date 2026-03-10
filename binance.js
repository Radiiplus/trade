#!/usr/bin/env node
/**
 * Binance Real-Time Market Feed Module
 * Retrieves live cryptocurrency market data from Binance public WebSocket API.
 *
 * Usage:
 *   const BinanceFeed = require('./binance');
 *   const feed = new BinanceFeed({ symbols: ['btcusdt', 'ethusdt'] });
 *   feed.on('trade', (data) => console.log(data));
 *   feed.connect();
 */

const WebSocket = require("ws");
const EventEmitter = require("events");
const https = require("https");

/**
 * Parse interval/period string to milliseconds.
 * Supports: s (seconds), m (minutes), h (hours), d (days), w (weeks), M (months), y (years)
 * @param {string} period - Period string (e.g., '15s', '1m', '5m', '1h', '1d', '1w', '1M', '1y')
 * @returns {number} Period in milliseconds
 */
function parsePeriod(period) {
  const match = period.match(/^(\d+)(s|m|h|d|w|M|y)$/);
  if (!match) {
    throw new Error(
      "Invalid period format. Use: 15s, 1m, 5m, 1h, 1d, 1w, 1M, 1y, etc.",
    );
  }
  const value = parseInt(match[1], 10);
  const unit = match[2];
  switch (unit) {
    case "s":
      return value * 1000;
    case "m":
      return value * 60 * 1000;
    case "h":
      return value * 60 * 60 * 1000;
    case "d":
      return value * 24 * 60 * 60 * 1000;
    case "w":
      return value * 7 * 24 * 60 * 60 * 1000;
    case "M":
      return value * 30 * 24 * 60 * 60 * 1000; // Approximate
    case "y":
      return value * 365 * 24 * 60 * 60 * 1000; // Approximate
    default:
      throw new Error("Invalid period unit: " + unit);
  }
}

/**
 * Parse interval string to milliseconds (alias for parsePeriod for backwards compatibility).
 * @param {string} interval - Interval string (e.g., '15s', '1m', '5m', '1h')
 * @returns {number} Interval in milliseconds
 */
function parseInterval(interval) {
  return parsePeriod(interval);
}

/**
 * Trade Aggregator - aggregates trades into custom interval candles.
 */
class TradeAggregator {
  constructor(intervalMs, onCandleUpdate, realTime = false) {
    this.intervalMs = intervalMs;
    this.onCandleUpdate = onCandleUpdate;
    this.realTime = realTime;
    this.candles = {}; // key: symbol, value: candle data
    this.timers = {}; // key: symbol, value: timer ID
  }

  /**
   * Get the start time of the current interval bucket.
   * @param {number} timestamp - Current timestamp in ms
   * @returns {number} Bucket start time in ms
   */
  getBucketStart(timestamp) {
    return Math.floor(timestamp / this.intervalMs) * this.intervalMs;
  }

  /**
   * Add a trade to the aggregator.
   * @param {Object} trade - Trade data
   */
  addTrade(trade) {
    const symbol = trade.symbol;
    const tradeTime = trade.timestamp.getTime();
    const bucketStart = this.getBucketStart(tradeTime);

    // Initialize candle if doesn't exist or new bucket
    if (
      !this.candles[symbol] ||
      this.candles[symbol].bucketStart !== bucketStart
    ) {
      // Close previous candle if exists
      if (this.candles[symbol]) {
        this._closeCandle(symbol);
      }

      // Create new candle
      this.candles[symbol] = {
        symbol: symbol,
        interval: this._formatInterval(this.intervalMs),
        bucketStart: bucketStart,
        openTime: new Date(bucketStart),
        closeTime: new Date(bucketStart + this.intervalMs),
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        volume: trade.quantity,
        quoteVolume: trade.price * trade.quantity,
        trades: 1,
        isClosed: false,
      };

      // Set timer to close candle at interval end
      const delay = bucketStart + this.intervalMs - tradeTime;
      this.timers[symbol] = setTimeout(() => {
        this._closeCandle(symbol);
      }, delay);
    } else {
      // Update existing candle
      const candle = this.candles[symbol];
      candle.high = Math.max(candle.high, trade.price);
      candle.low = Math.min(candle.low, trade.price);
      candle.close = trade.price;
      candle.volume += trade.quantity;
      candle.quoteVolume += trade.price * trade.quantity;
      candle.trades += 1;
    }

    // Emit real-time update if enabled
    if (this.realTime) {
      this.onCandleUpdate(this.candles[symbol]);
    }
  }

  /**
   * Close a candle and clean up.
   * @param {string} symbol - Symbol to close
   */
  _closeCandle(symbol) {
    if (this.candles[symbol]) {
      this.candles[symbol].isClosed = true;
      this.onCandleUpdate(this.candles[symbol]);
      delete this.candles[symbol];
    }
    if (this.timers[symbol]) {
      clearTimeout(this.timers[symbol]);
      delete this.timers[symbol];
    }
  }

  /**
   * Close all candles and clear timers.
   */
  closeAll() {
    for (const symbol of Object.keys(this.candles)) {
      this._closeCandle(symbol);
    }
  }

  /**
   * Format interval ms back to string.
   * @param {number} ms - Milliseconds
   * @returns {string} Interval string
   */
  _formatInterval(ms) {
    if (ms < 60000) {
      return ms / 1000 + "s";
    } else if (ms < 3600000) {
      return ms / 60000 + "m";
    } else if (ms < 86400000) {
      return ms / 3600000 + "h";
    } else {
      return ms / 86400000 + "d";
    }
  }
}

class BinanceFeed extends EventEmitter {
  /**
   * Create a Binance feed instance.
   * @param {Object} options - Configuration options
   * @param {string[]} options.symbols - Trading pairs (e.g., ['btcusdt', 'ethusdt'])
   * @param {string[]} options.streams - Stream types: 'trade', 'ticker', 'orderbook', 'kline' (default: all)
   * @param {string} options.interval - Kline interval: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M (default: '1m')
   * @param {string} options.period - Time period for kline history (not used in WebSocket, kept for API compatibility)
   * @param {string} options.aggregateInterval - Custom aggregation interval for trade-based candles (e.g., '15s', '30s', '1m'). Enables automatic candle aggregation from trades.
   * @param {boolean} options.realTime - Emit real-time candle updates during aggregation (default: false, only emits on candle close)
   */
  constructor(options = {}) {
    super();
    if (!options.symbols || !Array.isArray(options.symbols)) {
      throw new Error("symbols option is required and must be an array");
    }
    this.symbols = options.symbols.map((s) => s.toLowerCase());
    this.streams = options.streams || ["trade", "ticker", "orderbook"];
    this.interval = options.interval || "1m";
    this.period = options.period || null;
    this.aggregateInterval = options.aggregateInterval || null;
    this.realTime = options.realTime || false;
    this.aggregator = null;
    this.wsUrl = "wss://52.68.157.209/ws";
    this.ws = null;
    this.running = false;
    this.pingInterval = null;

    // Initialize aggregator if custom interval is set
    if (this.aggregateInterval) {
      const intervalMs = parseInterval(this.aggregateInterval);
      this.aggregator = new TradeAggregator(
        intervalMs,
        (candle) => {
          this.emit("kline", candle);
          this.emit("message", candle);
        },
        this.realTime,
      );
    }
  }

  /**
   * Build WebSocket URL with subscribed streams.
   * @returns {string} WebSocket URL
   */
  getStreamUrl() {
    const streams = [];
    for (const symbol of this.symbols) {
      if (this.streams.includes("trade")) {
        streams.push(symbol + "@trade");
      }
      if (this.streams.includes("ticker")) {
        streams.push(symbol + "@miniTicker");
      }
      if (this.streams.includes("orderbook")) {
        streams.push(symbol + "@depth5");
      }
      if (this.streams.includes("kline")) {
        streams.push(symbol + "@kline_" + this.interval);
      }
    }
    return this.wsUrl + "/" + streams.join("/");
  }

  /**
   * Process incoming WebSocket messages and emit events.
   * @param {Object} data - Parsed WebSocket message
   */
  processMessage(data) {
    if (!data.e) return;

    const eventType = data.e;
    const timestamp = new Date(data.E);

    if (eventType === "trade") {
      const trade = {
        type: "trade",
        symbol: data.s,
        timestamp: timestamp,
        price: parseFloat(data.p),
        quantity: parseFloat(data.q),
        side: data.m ? "BUY" : "SELL",
        tradeId: data.t,
        raw: data,
      };
      this.emit("trade", trade);
      this.emit("message", trade);

      // Aggregate trade if custom interval is set
      if (this.aggregator) {
        this.aggregator.addTrade(trade);
      }
    } else if (eventType === "24hrMiniTicker") {
      const ticker = {
        type: "ticker",
        symbol: data.s,
        timestamp: timestamp,
        close: parseFloat(data.c),
        open: parseFloat(data.o),
        high: parseFloat(data.h),
        low: parseFloat(data.l),
        volume: parseFloat(data.v),
        quoteVolume: parseFloat(data.q),
        changePercent: parseFloat(data.P),
        raw: data,
      };
      this.emit("ticker", ticker);
      this.emit("message", ticker);
    } else if (eventType === "depthUpdate") {
      if (
        data.bids &&
        data.bids.length > 0 &&
        data.asks &&
        data.asks.length > 0
      ) {
        const orderbook = {
          type: "orderbook",
          symbol: data.s,
          timestamp: timestamp,
          bids: data.bids.map((b) => ({
            price: parseFloat(b[0]),
            quantity: parseFloat(b[1]),
          })),
          asks: data.asks.map((a) => ({
            price: parseFloat(a[0]),
            quantity: parseFloat(a[1]),
          })),
          bestBid: parseFloat(data.bids[0][0]),
          bestAsk: parseFloat(data.asks[0][0]),
          spread: parseFloat(data.asks[0][0]) - parseFloat(data.bids[0][0]),
          raw: data,
        };
        this.emit("orderbook", orderbook);
        this.emit("message", orderbook);
      }
    } else if (eventType === "kline") {
      const kline = data.k;
      const candle = {
        type: "kline",
        symbol: data.s,
        interval: kline.i,
        timestamp: timestamp,
        openTime: new Date(kline.t),
        closeTime: new Date(kline.T),
        open: parseFloat(kline.o),
        high: parseFloat(kline.h),
        low: parseFloat(kline.l),
        close: parseFloat(kline.c),
        volume: parseFloat(kline.v),
        quoteVolume: parseFloat(kline.q),
        trades: kline.n,
        isClosed: kline.x,
        raw: data,
      };
      this.emit("kline", candle);
      this.emit("message", candle);
    }
  }

  /**
   * Connect to Binance WebSocket and start receiving data.
   * @returns {BinanceFeed} this
   */
  connect() {
    this.running = true;
    const url = this.getStreamUrl();

    this.emit("connecting", {
      url: url,
      symbols: this.symbols,
      streams: this.streams,
    });

    const agent = new https.Agent({ rejectUnauthorized: false });
    this.ws = new WebSocket(url, { agent });

    this.ws.on("open", () => {
      this.emit("connected", { url: url, symbols: this.symbols });
    });

    this.ws.on("message", (data) => {
      try {
        const parsed = JSON.parse(data);
        this.processMessage(parsed);
      } catch (err) {
        this.emit("error", new Error("JSON parse error: " + err.message));
      }
    });

    this.ws.on("error", (err) => {
      this.emit("error", err);
    });

    this.ws.on("close", () => {
      this.running = false;
      if (this.pingInterval) {
        clearInterval(this.pingInterval);
        this.pingInterval = null;
      }
      this.emit("disconnected");
    });

    // Keep-alive ping every 30 seconds
    this.pingInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.ping();
      }
    }, 30000);

    return this;
  }

  /**
   * Disconnect from Binance WebSocket.
   * @returns {BinanceFeed} this
   */
  stop() {
    this.running = false;
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
    if (this.aggregator) {
      this.aggregator.closeAll();
    }
    if (this.ws) {
      this.ws.close();
    }
    return this;
  }

  /**
   * Subscribe to additional symbols.
   * @param {string[]} symbols - New symbols to add
   * @returns {BinanceFeed} this
   */
  subscribe(symbols) {
    const newSymbols = symbols.map((s) => s.toLowerCase());
    for (const symbol of newSymbols) {
      if (!this.symbols.includes(symbol)) {
        this.symbols.push(symbol);
      }
    }
    if (this.running) {
      this.stop();
      this.connect();
    }
    return this;
  }

  /**
   * Unsubscribe from symbols.
   * @param {string[]} symbols - Symbols to remove
   * @returns {BinanceFeed} this
   */
  unsubscribe(symbols) {
    const removeSymbols = symbols.map((s) => s.toLowerCase());
    this.symbols = this.symbols.filter((s) => !removeSymbols.includes(s));
    if (this.running) {
      this.stop();
      this.connect();
    }
    return this;
  }

  /**
   * Make a REST API request to Binance.
   * @param {string} path - API endpoint path
   * @param {Object} params - Query parameters
   * @returns {Promise<Object>} Parsed JSON response
   * @private
   */
  _request(path, params = {}) {
    return new Promise((resolve, reject) => {
      const queryParams = new URLSearchParams(params).toString();
      const url = `https://api.binance.com${path}${queryParams ? "?" + queryParams : ""}`;

      https
        .get(url, (res) => {
          let data = "";
          res.on("data", (chunk) => {
            data += chunk;
          });
          res.on("end", () => {
            try {
              const parsed = JSON.parse(data);
              if (parsed.code && parsed.msg) {
                reject(new Error("Binance API Error: " + parsed.msg));
              } else {
                resolve(parsed);
              }
            } catch (err) {
              reject(new Error("JSON parse error: " + err.message));
            }
          });
        })
        .on("error", (err) => {
          reject(err);
        });
    });
  }

  /**
   * Get recent trades (up to 500).
   * @param {string} symbol - Trading pair (e.g., 'btcusdt')
   * @param {number} [limit=500] - Number of trades to return (max 500)
   * @returns {Promise<Object[]>} Array of recent trades
   * @example
   * const trades = await feed.getRecentTrades('btcusdt', 100);
   */
  async getRecentTrades(symbol, limit = 500) {
    if (!symbol) {
      throw new Error("symbol is required");
    }
    limit = Math.min(limit, 500);
    const trades = await this._request("/api/v3/trades", {
      symbol: symbol.toUpperCase(),
      limit: limit,
    });
    return trades.map((t) => ({
      id: t.id,
      price: parseFloat(t.price),
      quantity: parseFloat(t.qty),
      quoteQuantity: parseFloat(t.quoteQty),
      time: new Date(t.time),
      isBuyerMaker: t.isBuyerMaker,
      side: t.isBuyerMaker ? "SELL" : "BUY",
    }));
  }

  /**
   * Get historical trades.
   * @param {string} symbol - Trading pair (e.g., 'btcusdt')
   * @param {Object} options - Query options
   * @param {number} [options.fromId] - Trade ID to fetch from (exclusive)
   * @param {number} [options.limit] - Number of trades to return (max 500)
   * @returns {Promise<Object[]>} Array of historical trades
   * @example
   * const trades = await feed.getHistoricalTrades('btcusdt', { fromId: 123456789, limit: 100 });
   */
  async getHistoricalTrades(symbol, options = {}) {
    if (!symbol) {
      throw new Error("symbol is required");
    }
    const params = {
      symbol: symbol.toUpperCase(),
    };
    if (options.fromId) {
      params.fromId = options.fromId;
    }
    if (options.limit) {
      params.limit = Math.min(options.limit, 500);
    }
    const trades = await this._request("/api/v3/historicalTrades", params);
    return trades.map((t) => ({
      id: t.id,
      price: parseFloat(t.price),
      quantity: parseFloat(t.qty),
      quoteQuantity: parseFloat(t.quoteQty),
      time: new Date(t.time),
      isBuyerMaker: t.isBuyerMaker,
      side: t.isBuyerMaker ? "SELL" : "BUY",
    }));
  }

  /**
   * Get trades within a time range.
   * Note: This method fetches recent trades and filters by time client-side,
   * as Binance API doesn't support time-range queries for trades directly.
   * For large time ranges, use getHistoricalTrades with specific fromId values.
   * @param {string} symbol - Trading pair (e.g., 'btcusdt')
   * @param {Object} options - Query options
   * @param {number|Date} [options.startTime] - Start time in ms or Date object
   * @param {number|Date} [options.endTime] - End time in ms or Date object
   * @param {number} [options.limit] - Number of trades to fetch (max 500)
   * @returns {Promise<Object[]>} Array of trades within the time range (filtered client-side)
   * @example
   * const trades = await feed.getTradesByTimeRange('btcusdt', {
   *   startTime: Date.now() - 3600000, // 1 hour ago
   *   endTime: Date.now(),
   *   limit: 500
   * });
   */
  async getTradesByTimeRange(symbol, options = {}) {
    if (!symbol) {
      throw new Error("symbol is required");
    }

    const startTime =
      options.startTime instanceof Date
        ? options.startTime.getTime()
        : options.startTime;
    const endTime =
      options.endTime instanceof Date
        ? options.endTime.getTime()
        : options.endTime;
    const limit = Math.min(options.limit || 500, 500);

    // Fetch recent trades and filter by time client-side
    const allTrades = await this.getRecentTrades(symbol, limit);

    // Filter by time range
    return allTrades.filter((t) => {
      const time = t.time.getTime();
      return (!startTime || time >= startTime) && (!endTime || time <= endTime);
    });
  }

  /**
   * Get historical trades by period with automatic timestamp conversion.
   * This method converts period strings to exact timestamps for the Binance API.
   * @param {string} symbol - Trading pair (e.g., 'btcusdt')
   * @param {string} period - Time period (e.g., '1h', '24h', '7d', '1w', '1M', '1y')
   * @param {Date|number} [endTime=Date.now()] - End time (Date object or timestamp ms)
   * @param {number} [options.limit] - Number of trades per request (max 500)
   * @param {number} [options.maxResults] - Maximum total results to fetch
   * @returns {Promise<Object[]>} Array of historical trades
   * @example
   * const trades = await feed.getHistoryByPeriod('btcusdt', '1h'); // Last 1 hour
   * const trades = await feed.getHistoryByPeriod('btcusdt', '7d', { maxResults: 1000 }); // Last 7 days, max 1000 trades
   */
  async getHistoryByPeriod(symbol, period, endTime = Date.now(), options = {}) {
    if (!symbol) {
      throw new Error("symbol is required");
    }
    if (!period) {
      throw new Error(
        "period is required (e.g., '1h', '24h', '7d', '1w', '1M', '1y')",
      );
    }

    const periodMs = parsePeriod(period);
    const endTimestamp = endTime instanceof Date ? endTime.getTime() : endTime;
    const startTimestamp = endTimestamp - periodMs;
    const limit = Math.min(options.limit || 500, 500);
    const maxResults = options.maxResults || Infinity;

    const allTrades = await this.getRecentTrades(symbol, limit);

    // Filter by time range
    const filtered = allTrades.filter((t) => {
      const time = t.time.getTime();
      return time >= startTimestamp && time <= endTimestamp;
    });

    return filtered.slice(0, maxResults);
  }

  /**
   * Get trades from a specific period in the past.
   * Note: This fetches recent trades and filters by time client-side.
   * For older trades, use getHistoricalTrades with specific fromId values.
   * @param {string} symbol - Trading pair (e.g., 'btcusdt')
   * @param {string} period - Time period (e.g., '1h', '24h', '7d', '1w', '1M', '1y')
   * @param {number} [options.limit] - Number of trades to fetch (max 500)
   * @returns {Promise<Object[]>} Array of trades within the period
   * @example
   * const trades = await feed.getTradesByPeriod('btcusdt', '1h'); // Last 1 hour
   * const trades = await feed.getTradesByPeriod('btcusdt', '7d'); // Last 7 days
   */
  async getTradesByPeriod(symbol, period, options = {}) {
    if (!symbol) {
      throw new Error("symbol is required");
    }
    if (!period) {
      throw new Error(
        "period is required (e.g., '1h', '24h', '7d', '1w', '1M', '1y')",
      );
    }

    const periodMs = parsePeriod(period);
    const endTime =
      options.endTime instanceof Date
        ? options.endTime.getTime()
        : options.endTime || Date.now();
    const startTime = endTime - periodMs;
    // Fetch max trades to increase chance of finding trades within period
    const limit = 500;

    const allTrades = await this.getRecentTrades(symbol, limit);

    // Filter by time range
    return allTrades.filter((t) => {
      const time = t.time.getTime();
      return time >= startTime && time <= endTime;
    });
  }
}

// CLI execution
if (require.main === module) {
  const args = process.argv.slice(2);
  let symbols = null;
  let interval = "1m";
  let period = null;
  let aggregateInterval = null;
  let realTime = false;
  let historyMode = null;
  let startTime = null;
  let endTime = null;
  let limit = 500;
  let fromId = null;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--single" && args[i + 1]) {
      symbols = [args[i + 1].toLowerCase()];
      i++;
    } else if (args[i] === "--symbols") {
      symbols = [];
      while (i + 1 < args.length && !args[i + 1].startsWith("--")) {
        symbols.push(args[++i].toLowerCase());
      }
    } else if (args[i] === "--interval" && args[i + 1]) {
      interval = args[i + 1].toLowerCase();
      i++;
    } else if (args[i] === "--period" && args[i + 1]) {
      period = args[i + 1];
      i++;
    } else if (args[i] === "--aggregate" && args[i + 1]) {
      aggregateInterval = args[i + 1].toLowerCase();
      i++;
    } else if (args[i] === "--realtime") {
      realTime = true;
    } else if (args[i] === "--recent") {
      historyMode = "recent";
    } else if (args[i] === "--history") {
      historyMode = "history";
    } else if (args[i] === "--history-period" && args[i + 1]) {
      historyMode = "history-period";
      period = args[i + 1];
      i++;
    } else if (args[i] === "--time-range") {
      historyMode = "time-range";
    } else if (args[i] === "--last" && args[i + 1]) {
      historyMode = "period";
      period = args[i + 1];
      i++;
    } else if (args[i] === "--start-time" && args[i + 1]) {
      startTime = args[i + 1];
      i++;
    } else if (args[i] === "--end-time" && args[i + 1]) {
      endTime = args[i + 1];
      i++;
    } else if (args[i] === "--from-id" && args[i + 1]) {
      fromId = parseInt(args[i + 1], 10);
      i++;
    } else if (args[i] === "--limit" && args[i + 1]) {
      limit = parseInt(args[i + 1], 10);
      i++;
    }
  }

  // Handle history mode
  if (historyMode) {
    if (!symbols || symbols.length === 0) {
      console.log("Error: --single or --symbols is required for history mode");
      process.exit(1);
    }

    const feed = new BinanceFeed({ symbols });
    const symbol = symbols[0];

    (async () => {
      try {
        let trades;
        if (historyMode === "recent") {
          console.log(
            `Fetching ${limit} recent trades for ${symbol.toUpperCase()}...\n`,
          );
          trades = await feed.getRecentTrades(symbol, limit);
        } else if (historyMode === "history") {
          console.log(
            `Fetching historical trades for ${symbol.toUpperCase()}...\n`,
          );
          trades = await feed.getHistoricalTrades(symbol, { fromId, limit });
        } else if (historyMode === "time-range") {
          if (!startTime || !endTime) {
            console.log(
              "Error: --start-time and --end-time are required for time-range mode",
            );
            console.log(
              "Time format: ISO 8601 (e.g., 2024-01-01T00:00:00Z) or Unix timestamp in ms",
            );
            process.exit(1);
          }
          const start = new Date(startTime).getTime();
          const end = new Date(endTime).getTime();
          console.log(
            `Fetching trades for ${symbol.toUpperCase()} from ${new Date(start).toISOString()} to ${new Date(end).toISOString()}...\n`,
          );
          trades = await feed.getTradesByTimeRange(symbol, {
            startTime: start,
            endTime: end,
            maxResults: limit,
          });
        } else if (historyMode === "period") {
          if (!period) {
            console.log(
              "Error: --last is required (e.g., 1h, 24h, 7d, 1w, 1M, 1y)",
            );
            process.exit(1);
          }
          console.log(
            `Fetching trades for ${symbol.toUpperCase()} from the last ${period}...\n`,
          );
          trades = await feed.getTradesByPeriod(symbol, period, { limit });
        } else if (historyMode === "history-period") {
          if (!period) {
            console.log(
              "Error: --history-period is required (e.g., 1h, 24h, 7d, 1w, 1M, 1y)",
            );
            process.exit(1);
          }
          const periodMs = parsePeriod(period);
          const endTimestamp = Date.now();
          const startTimestamp = endTimestamp - periodMs;
          const startDate = new Date(startTimestamp);
          const endDate = new Date(endTimestamp);
          console.log(
            `Fetching trades for ${symbol.toUpperCase()} from ${startDate.toISOString()} to ${endDate.toISOString()} (last ${period})...\n`,
          );
          trades = await feed.getHistoryByPeriod(symbol, period, endDate, {
            limit,
          });
        }

        if (trades.length === 0) {
          console.log("No trades found.");
        } else {
          console.log(`Found ${trades.length} trades:\n`);
          console.log(
            "[ID]".padEnd(15) +
              "[TIME]".padEnd(20) +
              "[PRICE]".padEnd(15) +
              "[QTY]".padEnd(15) +
              "[SIDE]",
          );
          console.log("-".repeat(80));
          for (const trade of trades) {
            const timeStr =
              trade.time.toLocaleTimeString("en-US", { hour12: false }) +
              " " +
              trade.time.toISOString().split("T")[0];
            console.log(
              String(trade.id).padEnd(15) +
                timeStr.padEnd(20) +
                "$" +
                trade.price
                  .toLocaleString(undefined, {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                  })
                  .padEnd(14) +
                String(trade.quantity).padEnd(15) +
                trade.side,
            );
          }
        }
      } catch (err) {
        console.error("Error:", err.message);
      } finally {
        process.exit(0);
      }
    })();

    return;
  }

  if (!symbols) {
    console.log(
      "Usage: node binance.js --single <symbol> OR --symbols <symbol1> <symbol2> ...",
    );
    console.log("Example: node binance.js --symbols btcusdt ethusdt");
    console.log("\nOptions:");
    console.log("  Real-time Feed:");
    console.log(
      "    --interval <interval>       Kline interval: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M",
    );
    console.log(
      "    --aggregate <interval>      Custom aggregation from trades: 15s, 30s, 1m, 5m, etc.",
    );
    console.log(
      "    --realtime                  Emit real-time candle updates (default: false, only on close)",
    );
    console.log("\n  Historical Trades:");
    console.log(
      "    --recent                    Fetch recent trades (up to 500)",
    );
    console.log(
      "    --history-period <period>   Fetch trades by period with auto timestamp conversion",
    );
    console.log(
      "    --history                   Fetch historical trades by trade ID",
    );
    console.log(
      "    --time-range                Fetch recent trades filtered by time (max 500)",
    );
    console.log(
      "    --last <period>             Fetch trades from last period (1h, 24h, 7d, 1w, 1M, 1y)",
    );
    console.log(
      "    --from-id <id>              Starting trade ID for --history mode",
    );
    console.log(
      "    --start-time <time>         Start time (e.g., 2024-01-01, 2024-01-01T00:00:00Z, or timestamp ms)",
    );
    console.log(
      "    --end-time <time>           End time (e.g., 2024-01-02, 2024-01-02T00:00:00Z, or timestamp ms)",
    );
    console.log(
      "    --limit <count>             Number of trades to fetch (max 500, default: 500)",
    );
    console.log(
      "\n  Note: Binance API doesn't support time-range queries for trades.",
    );
    console.log(
      "  --time-range and --last filter recent trades client-side (max 500).",
    );
    console.log(
      "  For older trades, use --history with specific --from-id values.",
    );
    console.log("\n  Other:");
    console.log(
      "    --period <period>           Time period (for API compatibility)",
    );
    console.log(
      "  --streams <streams>         Stream types: trade, ticker, orderbook, kline (comma-separated)",
    );
    process.exit(1);
  }

  // Build streams list - don't include native kline if using aggregation
  const streams = aggregateInterval
    ? ["trade", "ticker", "orderbook"]
    : ["trade", "ticker", "orderbook", "kline"];

  const feed = new BinanceFeed({
    symbols,
    interval,
    period,
    aggregateInterval,
    realTime,
    streams,
  });

  feed.on("connected", (info) => {
    console.log("Connected! Listening for market data...\n");
  });

  feed.on("trade", (data) => {
    console.log(
      "[TRADE] " +
        data.timestamp.toLocaleTimeString("en-US", { hour12: false }) +
        " | " +
        data.symbol +
        " | " +
        "Price: $" +
        data.price.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "Qty: " +
        data.quantity +
        " | " +
        "Side: " +
        data.side,
    );
  });

  feed.on("ticker", (data) => {
    console.log(
      "[TICKER] " +
        data.timestamp.toLocaleTimeString("en-US", { hour12: false }) +
        " | " +
        data.symbol +
        " | " +
        "Last: $" +
        data.close.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "Change: " +
        data.changePercent.toFixed(2) +
        "% | " +
        "High: $" +
        data.high.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "Low: $" +
        data.low.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "Vol: " +
        data.volume.toLocaleString(),
    );
  });

  feed.on("orderbook", (data) => {
    console.log(
      "[ORDERBOOK] " +
        data.timestamp.toLocaleTimeString("en-US", { hour12: false }) +
        " | " +
        data.symbol +
        " | " +
        "Bid: $" +
        data.bestBid.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "Ask: $" +
        data.bestAsk.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "Spread: $" +
        data.spread.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }),
    );
  });

  feed.on("kline", (data) => {
    const status = data.isClosed ? "[CLOSED]" : "[OPEN] ";
    const time = data.openTime || data.timestamp;
    console.log(
      status +
        " [KLINE] " +
        time.toLocaleTimeString("en-US", { hour12: false }) +
        " | " +
        data.symbol +
        " | " +
        "Interval: " +
        data.interval +
        " | " +
        "O: $" +
        data.open.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "H: $" +
        data.high.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "L: $" +
        data.low.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "C: $" +
        data.close.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }) +
        " | " +
        "Vol: " +
        data.volume.toLocaleString(),
    );
  });

  feed.on("error", (err) => {
    console.error("Error:", err.message);
  });

  feed.on("disconnected", () => {
    console.log("\nDisconnected from Binance");
  });

  process.on("SIGINT", () => {
    console.log("\nInterrupted by user");
    feed.stop();
    process.exit(0);
  });

  feed.connect();
}

module.exports = BinanceFeed;
