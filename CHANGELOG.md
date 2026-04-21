# CHANGELOG — RSI-V5-2 量能显示与计算修复

## V2 修复(本次,在 V1 基础上增补)

### 🔴 量能虚高问题(用户反馈)

Dashboard 上某些币的 Buy/Sell 数字明显高于实际 5 分钟窗口内的真实成交量。经过深入排查,发现 **5 个独立的放大源**:

#### 问题 1:`heliusWs._extractTrade` WSOL `Math.abs` 累加(最严重)

```javascript
// 原版(错)
for (const wp of wsolPost) {
  wsolNetDelta += Math.abs(postAmt - preAmt);  // ← 用户-1 + pool+1 = 2
}
```

一笔 1 SOL 的 AMM swap,用户账户 WSOL -1,池子 WSOL +1,`Math.abs` 相加变成 2。**1 SOL 被算成 2 SOL**。多跳路由(Raydium 2 hop)甚至被放大 4 倍。

**修复**:改为分别累加正/负 delta,取较小的绝对值(= 真实用户侧成交额)。

#### 问题 2:native SOL 扫所有账户取 max

原版 `maxNativeSolChange` 会误把 Jito MEV tip 账户、priority fee 等变化当作交易金额。

**修复**:加 `MIN_SOL_DELTA = 0.0001` 过滤器,忽略微小变化。

#### 问题 3:两条 SOL 策略 Math.max 相当于采纳较大的那个

原版 `solAmount = Math.max(nativeSolChange, wsolNetDelta)`。WSOL 被放大 2x 后往往 > native,于是采纳放大后的值。

**修复**:两条策略都有数据时取较小者(真实成交额),差距 10 倍以上视为 fee 噪音采纳较大值。

#### 问题 4:V1 修复引入的 `Math.max(kBuy, tBuy)` 合并

我在 V1 修复里加的"K线路径 vs tick路径取 max"逻辑,在两条路径窗口不对齐时会造成虚高。

**修复**:显示层只用一条路径 — `_refreshLiveVolume` 严格 VOL_WINDOW_SEC 秒滑动窗口。K 线聚合仅用于 RSI 信号判断,两者职责分离。

#### 问题 5:K线 currentCandle 窗口随机

原版显示"300s 窗口"实际上取的是 currentCandle(当前未收盘K线),实际已过时间是 0~300 秒随机。K 线刚翻转的瞬间显示的是最近 1 秒的数据却标注为 300s。

**修复**:同问题 4,改走严格时间窗口。

### 🔧 本次修改文件

| 文件 | 改动 |
|---|---|
| `src/heliusWs.js` | 重写 `_extractTrade`,修复 3 条 SOL 金额计算路径 |
| `src/monitor.js` | 去掉 `Math.max` 合并;显示统一用 `_refreshLiveVolume` 严格时间窗口;加节流和 txCount 字段;诊断日志对比两条路径 |
| `public/index.html` | tooltip 带上窗口秒数和交易笔数 |

### 🔍 如何验证修复有效

启动服务后,看 `logs/*.log` 里每 60 秒一次的诊断行:

```
[VolDiag] SYMBOL | chainTicks:all=120,win=45 | tick路径=B12.5/S8.3 | K线路径=B11.9/S8.1 | win=300s
```

- `tick路径` 和 `K线路径` 应该非常接近(差 < 20%)
- Dashboard 显示应对上 `tick路径` 的数字
- 与 GMGN / DexScreener 的 5 分钟成交量对比,应吻合(误差 5% 以内)

如果 `tick路径` 仍明显偏高,可能是 Helius 批量订阅误匹配其他代币,临时设置 `.env` 里 `HELIUS_BATCH_THRESHOLD=200` 强制独立订阅模式。

---

## V1 修复(上一轮)

### Buy/Sell 列大量显示 `-` 问题

修复了 4 个问题:
- 前端 `handleTokenList` 只删不写(最关键)
- 后端 `_stateSnapshot` 快照缺字段
- `evaluateSignal` 预热分支返回空 volume
- `calcVolumeInfo` 兜底只取单根K线导致失真

修改文件:`src/rsi.js`、`src/monitor.js`、`public/index.html`

---

## 部署

无数据库迁移,直接替换源码重启:

```bash
git pull
npm install --omit=dev
sudo systemctl restart sol-rsi-monitor

# 查看量能诊断
journalctl -u sol-rsi-monitor -f | grep VolDiag
```

## 可选调参

```bash
# .env 里
VOL_WINDOW_SEC=300             # 量能窗口(秒),默认 300 即 5 分钟
HELIUS_BATCH_THRESHOLD=200     # 批量订阅阈值,调大强制走独立订阅
```
