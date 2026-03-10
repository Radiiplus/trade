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
 * Parse interval string to milliseconds.
 * @param {string} interval - Interval string (e.g., '15s', '1m', '5m', '1h')
 * @returns {number} Interval in milliseconds
 */
function parseInterval(interval) {
  const match = interval.match(/^(\d+)(s|m|h|d)$/);
  if (!match) {
    throw new Error("Invalid interval format. Use: 15s, 1m, 5m, 1h, etc.");
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
    default:
      throw new Error("Invalid interval unit: " + unit);
  }
}

/**
 * Trade Aggregator - aggregates trades into custom interval candles.
 */
class TradeAggregator {
  constructor(intervalMs, onCandleUpdate) {
    this.intervalMs = intervalMs;
    this.onCandleUpdate = onCandleUpdate;
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

    // Emit update
    this.onCandleUpdate(this.candles[symbol]);
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
    this.aggregator = null;
    this.wsUrl = "wss://52.68.157.209/ws";
    this.ws = null;
    this.running = false;
    this.pingInterval = null;

    // Initialize aggregator if custom interval is set
    if (this.aggregateInterval) {
      const intervalMs = parseInterval(this.aggregateInterval);
      this.aggregator = new TradeAggregator(intervalMs, (candle) => {
        this.emit("kline", candle);
        this.emit("message", candle);
      });
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
}

// CLI execution
if (require.main === module) {
  const args = process.argv.slice(2);
  let symbols = null;
  let interval = "1m";
  let period = null;
  let aggregateInterval = null;

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
    }
  }

  if (!symbols) {
    console.log(
      "Usage: node binance.js --single <symbol> OR --symbols <symbol1> <symbol2> ...",
    );
    console.log("Example: node binance.js --symbols btcusdt ethusdt");
    console.log("Options:");
    console.log(
      "  --interval <interval>       Kline interval: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M",
    );
    console.log(
      "  --aggregate <interval>      Custom aggregation from trades: 15s, 30s, 1m, 5m, etc.",
    );
    console.log(
      "  --period <period>           Time period (for API compatibility)",
    );
    console.log(
      "  --streams <streams>         Stream types: trade, ticker, orderbook, kline (comma-separated)",
    );
    process.exit(1);
  }

  const feed = new BinanceFeed({
    symbols,
    interval,
    period,
    aggregateInterval,
    streams: ["trade", "ticker", "orderbook", "kline"],
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
    console.log(
      status +
        " [KLINE] " +
        data.timestamp.toLocaleTimeString("en-US", { hour12: false }) +
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
