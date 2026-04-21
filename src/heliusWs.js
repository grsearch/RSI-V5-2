'use strict';
// src/heliusWs.js — Helius Enhanced WebSocket 链上交易监听 V5
//
// 订阅策略（统一支持所有 AMM：Pump/Raydium/Meteora/Orca）：
//
//   代币数 ≤ BATCH_THRESHOLD（默认30）→ 独立订阅（每个 token 一个 subscription）
//   代币数 > BATCH_THRESHOLD         → 批量订阅（所有 mint 放入一个 accountInclude 数组）
//
//   已彻底移除 pump 模式（只支持 Pump AMM，不适合混合 AMM 场景）。

const WebSocket = require('ws');
const logger    = require('./logger');

const HELIUS_WSS_URL        = process.env.HELIUS_WSS_URL || '';
const HELIUS_GATEKEEPER_URL = process.env.HELIUS_GATEKEEPER_URL || '';
const HELIUS_API_KEY        = process.env.HELIUS_API_KEY || '';
const HELIUS_RPC_URL        = process.env.HELIUS_RPC_URL || '';

// 超过此数量时改用批量订阅
const BATCH_THRESHOLD = parseInt(process.env.HELIUS_BATCH_THRESHOLD || '30', 10);

const LAMPORTS     = 1e9;
const PING_MS      = 25000;
const RECONNECT_MS = 2000;
const MAX_RETRIES  = 999;

function getWsUrl() {
  if (HELIUS_GATEKEEPER_URL) {
    let url = HELIUS_GATEKEEPER_URL;
    if (url.startsWith('https://')) url = url.replace('https://', 'wss://');
    if (!url.startsWith('wss://')) url = 'wss://' + url;
    return { url, type: 'gatekeeper' };
  }
  if (HELIUS_WSS_URL) return { url: HELIUS_WSS_URL, type: 'enhanced' };
  const apiKey = HELIUS_API_KEY || extractApiKey(HELIUS_RPC_URL);
  if (!apiKey) return { url: '', type: 'none' };
  return { url: 'wss://mainnet.helius-rpc.com/?api-key=' + apiKey, type: 'enhanced' };
}

function extractApiKey(rpcUrl) {
  const m = (rpcUrl || '').match(/api-key=([a-f0-9-]+)/i);
  return m ? m[1] : '';
}

class HeliusTradeStream {
  constructor() {
    this._ws          = null;
    this._pingTimer   = null;
    this._statsTimer  = null;
    this._connected   = false;
    this._retryCount  = 0;
    this._connType    = 'none';
    this._tokens      = new Map(); // address → { symbol, onTrade, subId }
    this._pendingSubs = new Map(); // rpcId → address | '__batch__'
    this._nextRpcId   = 100;
    this._batchSubIds   = [];
    this._batchSubId    = null;
    this._batchDebounce    = null;
    this._batchTimeoutTimer = null;
    this._CHUNK_SIZE        = 50;
    this._stats = { txReceived: 0, txMatched: 0, txParsed: 0, txSkipped: 0, connType: 'none' };
  }

  start() {
    const { url, type } = getWsUrl();
    if (!url) {
      logger.warn('[HeliusWS] 未配置 Helius WebSocket URL，链上量能数据不可用');
      return;
    }
    this._connType = type;
    this._stats.connType = type;
    logger.info(`[HeliusWS] 启动 | 批量订阅阈值=${BATCH_THRESHOLD}`);
    this._connect(url);
  }

  stop() {
    this._connected = false;
    this._retryCount = MAX_RETRIES + 1;
    if (this._pingTimer)  { clearInterval(this._pingTimer);  this._pingTimer  = null; }
    if (this._statsTimer) { clearInterval(this._statsTimer); this._statsTimer = null; }
    if (this._ws) { try { this._ws.close(); } catch (_) {} this._ws = null; }
  }

  _connect(wsUrl) {
    const safeUrl = wsUrl.replace(/api-key=[a-f0-9-]+/i, 'api-key=***');
    logger.info('[HeliusWS] 连接 %s ...', safeUrl);

    if (!this._statsTimer) {
      this._statsTimer = setInterval(() => {
        const s = this.getStats();
        logger.info(`[HeliusWS] 状态: tokens=${s.tokens} subMode=${s.subMode} batchSubId=${s.batchSubId||'none'} txReceived=${s.txReceived} txMatched=${s.txMatched} txParsed=${s.txParsed}`);
      }, 60000);
    }

    this._ws = new WebSocket(wsUrl);

    this._ws.on('open', () => {
      logger.info('[HeliusWS] ✅ 已连接 (%s)', this._connType);
      this._connected  = true;
      this._retryCount = 0;
      this._batchSubId  = null;
      this._batchSubIds = [];

      this._pingTimer = setInterval(() => {
        if (this._ws && this._ws.readyState === WebSocket.OPEN) this._ws.ping();
      }, PING_MS);

      this._resubscribeAll();
    });

    this._ws.on('message', (data) => this._handleMessage(data));
    this._ws.on('pong', () => {});
    this._ws.on('error', (err) => logger.error(`[HeliusWS] 错误: ${err.message}`));

    this._ws.on('close', () => {
      logger.warn('[HeliusWS] 连接关闭');
      this._connected  = false;
      this._batchSubId  = null;
      this._batchSubIds = [];
      this._pendingSubs.clear();
      if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }

      if (this._retryCount < MAX_RETRIES) {
        this._retryCount++;
        const delay = Math.min(RECONNECT_MS * Math.pow(1.5, this._retryCount - 1), 30000);
        logger.info(`[HeliusWS] ${(delay/1000).toFixed(0)}s 后重连 (第${this._retryCount}次)`);
        setTimeout(() => {
          const { url } = getWsUrl();
          if (url) this._connect(url);
        }, delay);
      }
    });
  }

  _resubscribeAll() {
    if (this._tokens.size === 0) return;
    for (const info of this._tokens.values()) info.subId = null;

    if (this._tokens.size > BATCH_THRESHOLD) {
      setTimeout(() => this._subscribeBatch(), 1000);
    } else {
      let i = 0;
      for (const [address] of this._tokens.entries()) {
        setTimeout(() => {
          if (this._tokens.has(address) && this._connected) this._subscribeToken(address);
        }, i * 150);
        i++;
      }
    }
  }

  _subscribeToken(tokenAddress) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    const rpcId = this._nextRpcId++;
    this._pendingSubs.set(rpcId, tokenAddress);
    this._ws.send(JSON.stringify({
      jsonrpc: '2.0', id: rpcId,
      method: 'transactionSubscribe',
      params: [
        { accountInclude: [tokenAddress], failed: false },
        { commitment: 'confirmed', encoding: 'jsonParsed', transactionDetails: 'full', maxSupportedTransactionVersion: 0 },
      ],
    }));
    const info = this._tokens.get(tokenAddress);
    logger.debug(`[HeliusWS] 独立订阅 ${(info && info.symbol) || tokenAddress.slice(0,8)}`);
  }

  _unsubscribeToken(tokenAddress) {
    const info = this._tokens.get(tokenAddress);
    if (!info || !info.subId) return;
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify({
        jsonrpc: '2.0', id: this._nextRpcId++,
        method: 'transactionUnsubscribe',
        params: [info.subId],
      }));
    }
    info.subId = null;
  }

  _subscribeBatch() {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    const addresses = Array.from(this._tokens.keys());
    if (addresses.length === 0) return;

    // 取消所有旧的批量订阅
    for (const oldSubId of this._batchSubIds) {
      this._ws.send(JSON.stringify({
        jsonrpc: '2.0', id: this._nextRpcId++,
        method: 'transactionUnsubscribe',
        params: [oldSubId],
      }));
    }
    this._batchSubIds = [];
    this._batchSubId  = null;

    // 取消所有独立订阅
    for (const info of this._tokens.values()) {
      if (info.subId) {
        this._ws.send(JSON.stringify({
          jsonrpc: '2.0', id: this._nextRpcId++,
          method: 'transactionUnsubscribe',
          params: [info.subId],
        }));
        info.subId = null;
      }
    }

    // 分块发送批量订阅（每块最多 CHUNK_SIZE 个地址）
    const chunks = [];
    for (let i = 0; i < addresses.length; i += this._CHUNK_SIZE) {
      chunks.push(addresses.slice(i, i + this._CHUNK_SIZE));
    }

    logger.info(`[HeliusWS] 📡 批量订阅 ${addresses.length} 个 token，分 ${chunks.length} 块`);

    chunks.forEach((chunk, idx) => {
      const rpcId = this._nextRpcId++;
      this._pendingSubs.set(rpcId, `__batch_${idx}__`);
      this._ws.send(JSON.stringify({
        jsonrpc: '2.0', id: rpcId,
        method: 'transactionSubscribe',
        params: [
          { accountInclude: chunk, failed: false },
          { commitment: 'confirmed', encoding: 'jsonParsed', transactionDetails: 'full', maxSupportedTransactionVersion: 0 },
        ],
      }));
      logger.info(`[HeliusWS] 📡 块[${idx+1}/${chunks.length}] ${chunk.length} 个 token rpcId=${rpcId}`);
    });

    // ★ 超时降级：若5秒内未收到任何批量订阅确认，自动回退到独立订阅
    clearTimeout(this._batchTimeoutTimer);
    this._batchTimeoutTimer = setTimeout(() => {
      if (this._batchSubIds.length === 0 && this._connected) {
        logger.warn('[HeliusWS] ⚠️ 批量订阅5秒内未确认，降级到独立订阅模式');
        this._fallbackToIndividual();
      }
    }, 5000);
  }

  _fallbackToIndividual() {
    let i = 0;
    for (const [addr] of this._tokens.entries()) {
      setTimeout(() => {
        if (this._tokens.has(addr) && this._connected) this._subscribeToken(addr);
      }, i * 100);
      i++;
    }
    logger.info(`[HeliusWS] 🔄 降级独立订阅 ${this._tokens.size} 个 token`);
  }

  subscribe(tokenAddress, symbol, onTrade) {
    this._tokens.set(tokenAddress, { symbol, onTrade, subId: null });
    const count = this._tokens.size;

    if (this._connected) {
      if (count > BATCH_THRESHOLD) {
        clearTimeout(this._batchDebounce);
        this._batchDebounce = setTimeout(() => {
          if (this._connected) this._subscribeBatch();
        }, 3000);
      } else {
        setTimeout(() => {
          if (this._tokens.has(tokenAddress) && this._connected) this._subscribeToken(tokenAddress);
        }, 50);
      }
    }
    logger.info(`[HeliusWS] 📌 注册 ${symbol}，当前监控 ${count} 个`);
  }

  unsubscribe(tokenAddress) {
    if (!this._batchSubId) this._unsubscribeToken(tokenAddress);
    this._tokens.delete(tokenAddress);

    if (this._batchSubId && this._connected) {
      clearTimeout(this._batchDebounce);
      this._batchDebounce = setTimeout(() => {
        if (this._connected) this._subscribeBatch();
      }, 1000);
    }
    logger.info(`[HeliusWS] 🔕 移除 ${tokenAddress.slice(0,8)}，剩余 ${this._tokens.size} 个`);
  }

  _handleMessage(rawData) {
    let msg;
    try { msg = JSON.parse(rawData.toString('utf8')); } catch (_) { return; }

    if (msg.id && msg.result !== undefined) {
      const key = this._pendingSubs.get(msg.id);
      if (!key) return;
      this._pendingSubs.delete(msg.id);

      if (key.startsWith('__batch_')) {
        if (typeof msg.result === 'number') {
          this._batchSubIds.push(msg.result);
          if (!this._batchSubId) this._batchSubId = msg.result;
          clearTimeout(this._batchTimeoutTimer); // 收到确认，取消降级计时
          logger.info(`[HeliusWS] ✅ 批量订阅块确认 subId=${msg.result} (共 ${this._batchSubIds.length} 块)`);
        } else {
          logger.warn(`[HeliusWS] ❌ 批量订阅块失败: ${JSON.stringify(msg).slice(0,200)}`);
          // 降级：逐个独立订阅
          let i = 0;
          for (const [addr] of this._tokens.entries()) {
            setTimeout(() => {
              if (this._tokens.has(addr) && this._connected) this._subscribeToken(addr);
            }, i * 100);
            i++;
          }
        }
      } else {
        const info = this._tokens.get(key);
        if (info) {
          info.subId = msg.result;
          logger.debug(`[HeliusWS] ✅ 独立订阅确认 ${key.slice(0,8)} subId=${msg.result}`);
        }
      }
      return;
    }

    if (msg.method === 'transactionNotification' && msg.params && msg.params.result) {
      this._stats.txReceived++;
      this._parseTransaction(msg.params.result);
    }
  }

  _parseTransaction(result) {
    try {
      const txWrapper = result.transaction;
      if (!txWrapper) return;
      const meta   = txWrapper.meta;
      const txData = txWrapper.transaction;
      if (!meta || meta.err) return;

      const postTokenBals = meta.postTokenBalances || [];
      if (postTokenBals.length === 0) return;

      const involvedMints = new Set(postTokenBals.map(b => b.mint).filter(Boolean));
      let matched = false;

      for (const mint of involvedMints) {
        const tokenInfo = this._tokens.get(mint);
        if (!tokenInfo) continue;
        matched = true;
        this._stats.txMatched++;
        const trade = this._extractTrade(mint, meta, txData, result.signature);
        if (trade) {
          this._stats.txParsed++;
          tokenInfo.onTrade(trade);
        }
      }

      if (!matched) this._stats.txSkipped++;
    } catch (err) {
      logger.debug(`[HeliusWS] 解析交易失败: ${err.message}`);
    }
  }

  _extractTrade(tokenAddress, meta, txData, signature) {
    const WSOL = 'So11111111111111111111111111111111111111112';
    const preTokenBals  = meta.preTokenBalances  || [];
    const postTokenBals = meta.postTokenBalances  || [];
    const preBalances   = meta.preBalances  || [];
    const postBalances  = meta.postBalances || [];

    // ── 1. 找到"用户账户"的 token 净变化 ─────────────────────────
    //   AMM swap 的本质：一方(用户)的 token 余额变化 = 另一方(池子)变化的反数
    //   取用户侧(usually 非 pool、非 mint authority)的变化作为成交量基准
    const postEntries = postTokenBals.filter(b => b.mint === tokenAddress);
    const preEntries  = preTokenBals.filter(b => b.mint === tokenAddress);
    if (postEntries.length === 0) return null;

    // 计算每个账户的 delta
    const tokenDeltas = [];
    for (const postEntry of postEntries) {
      const preEntry = preEntries.find(b =>
        b.accountIndex === postEntry.accountIndex || b.owner === postEntry.owner);
      const postAmt = parseFloat((postEntry.uiTokenAmount && postEntry.uiTokenAmount.uiAmount) || '0');
      const preAmt  = preEntry ? parseFloat((preEntry.uiTokenAmount && preEntry.uiTokenAmount.uiAmount) || '0') : 0;
      const delta = postAmt - preAmt;
      if (Math.abs(delta) > 1e-9) {
        tokenDeltas.push({ delta, owner: postEntry.owner, accountIndex: postEntry.accountIndex });
      }
    }
    if (tokenDeltas.length === 0) return null;

    // ★ FIX: 正负 delta 应该大致相等（一方给，一方收），取绝对值最大者作为"用户侧"
    //   注意：多跳 swap 会产生多个 pool 账户变化，但单笔交易的用户净变化仍是唯一的
    //   正确口径是"所有正 delta 之和 与 所有负 delta 之和，取绝对值较小的那个"
    //   （较大的那个会因为路由过程中的临时累加而虚高）
    let sumPositive = 0, sumNegative = 0;
    for (const d of tokenDeltas) {
      if (d.delta > 0) sumPositive += d.delta;
      else sumNegative += d.delta;
    }
    // 成交 token 量 = min(|正变化总和|, |负变化总和|)
    //   理想情况两者相等；若不等(手续费、转账)，取较小者避免虚高
    const absTokenDelta = Math.min(Math.abs(sumPositive), Math.abs(sumNegative));
    if (absTokenDelta < 1e-9) return null;

    // 买卖方向：用户侧变化为正 = 买入；用户侧一般是绝对值最大的那个账户（非 pool）
    // 简化：绝对值最大的 delta 的符号代表用户侧方向
    let userDelta = tokenDeltas[0].delta;
    for (const d of tokenDeltas) {
      if (Math.abs(d.delta) > Math.abs(userDelta)) userDelta = d.delta;
    }
    const isBuy = userDelta > 0;

    // ── 2. 计算 SOL 金额（两条策略取其中"真实成交"的那个）───────
    //
    //   ★ 关键修复：原版用 Math.abs 累加所有 WSOL 账户变化，会把 1 SOL 成交
    //     算成 2 SOL(用户-1、pool+1 两边都 abs) 甚至更多(多跳路由累加)
    //
    //   正确口径：用户侧 SOL 变化 = 成交额
    //     native SOL: 单账户最大变化（Pump AMM 用户直接转 SOL）
    //     WSOL:       取所有 WSOL 账户正/负 delta 之和的较小绝对值
    //                 （和 token 同理：一方给一方收，min 才是真实成交额）

    // native SOL：扣掉 tip/fee 的影响，只看大额账户
    // 我们只关心"可能是用户"的变化，通常用户账户 SOL 变化 >= 0.0001 SOL 才算有效
    //   tip 账户(Jito MEV)一般 < 0.01 SOL，priority fee 通常 < 0.001 SOL
    //   取"第二大"而不是最大能更稳，但简化起见用最大且设下限
    const MIN_SOL_DELTA = 0.0001;
    let nativeSolDelta = 0;
    for (let i = 0; i < preBalances.length && i < postBalances.length; i++) {
      const delta = Math.abs(postBalances[i] - preBalances[i]) / LAMPORTS;
      if (delta > MIN_SOL_DELTA && delta > nativeSolDelta) nativeSolDelta = delta;
    }

    // WSOL: 正/负两侧分别求和，取较小绝对值
    let wsolSumPos = 0, wsolSumNeg = 0;
    const wsolPost = postTokenBals.filter(b => b.mint === WSOL);
    const wsolPre  = preTokenBals.filter(b => b.mint === WSOL);
    for (const wp of wsolPost) {
      const wr = wsolPre.find(b => b.accountIndex === wp.accountIndex || b.owner === wp.owner);
      const postAmt = parseFloat((wp.uiTokenAmount && wp.uiTokenAmount.uiAmount) || '0');
      const preAmt  = wr ? parseFloat((wr.uiTokenAmount && wr.uiTokenAmount.uiAmount) || '0') : 0;
      const d = postAmt - preAmt;
      if (d > 0) wsolSumPos += d;
      else wsolSumNeg += d;
    }
    // 处理 pre 里有但 post 里没了的 WSOL 账户（WSOL 账户被关闭 sync）
    for (const wr of wsolPre) {
      const found = wsolPost.find(b => b.accountIndex === wr.accountIndex || b.owner === wr.owner);
      if (!found) {
        const preAmt = parseFloat((wr.uiTokenAmount && wr.uiTokenAmount.uiAmount) || '0');
        if (preAmt > 0) wsolSumNeg -= preAmt;  // 账户被关，等同于 -preAmt
      }
    }
    const wsolNetDelta = Math.min(Math.abs(wsolSumPos), Math.abs(wsolSumNeg));

    // ── 3. 最终 solAmount：两条策略取其中"看起来合理"的那个 ─────
    //   策略选择逻辑：
    //   - Pump AMM 直接用 native SOL → nativeSolDelta 大、wsolNetDelta 为 0
    //   - Raydium/Meteora 用 WSOL   → wsolNetDelta 大、nativeSolDelta 只是 fee
    //   - 套利/MEV 混合              → 两者都有，取较小者更接近真实用户成交额
    //   取两者中有效且较小的那一个（>MIN_SOL_DELTA 才算有效）
    let solAmount = 0;
    const candidates = [nativeSolDelta, wsolNetDelta].filter(v => v > MIN_SOL_DELTA);
    if (candidates.length === 2) {
      // 两条策略都有数据：取较小者（避免策略相加导致翻倍）
      //   但如果较小的明显太小（< 较大的 10%）说明它是 fee，用较大者
      const [a, b] = candidates.sort((x, y) => x - y);
      solAmount = (a / b < 0.1) ? b : a;
    } else if (candidates.length === 1) {
      solAmount = candidates[0];
    }

    if (solAmount <= 0) return null;

    return {
      ts: Date.now(), signature, tokenAddress,
      owner: postEntries[0]?.owner || '',
      isBuy,
      solAmount,
      tokenAmount: absTokenDelta,
      priceSol: absTokenDelta > 0 ? solAmount / absTokenDelta : 0,
    };
  }

  isConnected() { return this._connected; }
  getSubscriptionCount() { return this._tokens.size; }

  getStats() {
    let confirmedSubs = 0;
    for (const info of this._tokens.values()) { if (info.subId) confirmedSubs++; }
    return {
      connected:     this._connected,
      connType:      this._connType,
      subMode:       this._batchSubId ? 'batch' : 'token',
      tokens:        this._tokens.size,
      confirmedSubs: this._batchSubId ? this._tokens.size : confirmedSubs,
      batchSubId:    this._batchSubId || null,
      batchActive:   this._batchSubIds.length > 0,
      retryCount:    this._retryCount,
      ...this._stats,
    };
  }
}

const heliusWs = new HeliusTradeStream();
module.exports = heliusWs;
