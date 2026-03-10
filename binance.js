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

class BinanceFeed extends EventEmitter {
  /**
   * Create a Binance feed instance.
   * @param {Object} options - Configuration options
   * @param {string[]} options.symbols - Trading pairs (e.g., ['btcusdt', 'ethusdt'])
   * @param {string[]} options.streams - Stream types: 'trade', 'ticker', 'orderbook' (default: all)
   */
  constructor(options = {}) {
    super();
    if (!options.symbols || !Array.isArray(options.symbols)) {
      throw new Error("symbols option is required and must be an array");
    }
    this.symbols = options.symbols.map((s) => s.toLowerCase());
    this.streams = options.streams || ["trade", "ticker", "orderbook"];
    this.wsUrl = "wss://52.68.157.209/ws";
    this.ws = null;
    this.running = false;
    this.pingInterval = null;
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

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--single" && args[i + 1]) {
      symbols = [args[i + 1].toLowerCase()];
      i++;
    } else if (args[i] === "--symbols") {
      symbols = [];
      while (i + 1 < args.length && !args[i + 1].startsWith("--")) {
        symbols.push(args[++i].toLowerCase());
      }
    }
  }

  if (!symbols) {
    console.log(
      "Usage: node binance.js --single <symbol> OR --symbols <symbol1> <symbol2> ...",
    );
    console.log("Example: node binance.js --symbols btcusdt ethusdt");
    process.exit(1);
  }

  const feed = new BinanceFeed({ symbols });

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
