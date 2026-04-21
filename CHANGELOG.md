# CHANGELOG — RSI-V5-2 修复

## V4 修复(本次)— 量能方向判断错误 + 95 币动态订阅管理

### 现象

| 币 | Dashboard 显示 | GMGN 实际 5 分钟 |
|---|---|---|
| Rudi | **Buy 9.81 / Sell 1.19 SOL** | 实际卖 ~3.1 SOL,买 ~0 SOL |
| (多币) | 买卖方向对反,金额也虚高 | - |

### 问题根源(3 条 `_extractTrade` bug)

#### ① 方向判断不稳定

```javascript
// 原版(错)
let userDelta = tokenDeltas[0].delta;
for (const d of tokenDeltas) {
  if (Math.abs(d.delta) > Math.abs(userDelta)) userDelta = d.delta;
}
const isBuy = userDelta > 0;
```

AMM swap 里用户 delta 和 pool delta 绝对值完全相等(或因手续费只差 0.01%)。`>` 不严格成立时 `userDelta` 保留遍历到的**第一个**——可能是 pool 侧的,**方向反了**。

**修复**:**用交易签名者(fee payer = 用户钱包)作为方向判断的锚点**。Solana 交易的 `message.accountKeys[0]` 就是签名者,取签名者账户的 token delta 正负就是 100% 准确的买/卖方向。

#### ② 多跳路由把别人的交易算到本币头上

Helius `accountInclude` 订阅是 account-level,一笔 `TOKEN_A → WSOL → Rudi` 的多跳会同时匹配 TOKEN_A 和 Rudi。原代码对 Rudi 调用 `_extractTrade`,拿到的是整个路由的 SOL,**用户实际用 TOKEN_A 买 Rudi,不是用 SOL**。

**修复**:判断签名者的 WSOL/native 变化是否接近 0,且涉及多个非 WSOL token,则跳过这笔(签名者不是在用 SOL 买卖本 token)。

#### ③ SOL 金额取值方向混乱

原版 `max(nativeSolDelta, wsolNetDelta)` 对中转账户也敏感,套利交易会放大。

**修复**:只看**签名者账户**的 native SOL 变化(扣除手续费)+ **签名者**的 WSOL ATA 变化之和。签名者路径失效时才退回 pool 侧兜底。

### 95 币动态订阅管理优化

**原问题**:每次 add/remove 都 `unsubscribe 旧 + subscribe 新`,中间有订阅空窗期,会丢交易。

**修复**:

1. **先建新订阅,后取消旧订阅**:新订阅发出后等 `3秒 + 块数*200ms` 再取消旧订阅,确保新订阅已确认并激活,旧期间的交易不会丢
2. **防抖延长到 5 秒**:95 币陆续进/出时 5 秒内合并为一次重订阅,大幅减少 API 调用
3. **`CHUNK_SIZE` 可配置**:通过 `HELIUS_CHUNK_SIZE` 环境变量调整,默认 50

### 增加的诊断

Monitor 里 SOL 量 ≥ 1.0 的大额交易改为 INFO 级别日志,方便人工核对 GMGN:

```
[HeliusTrade] Rudi SELL 3.142 SOL @ 0.00000012 (5gx7zk...)
```

看日志里是 SELL 就对,和你 GMGN 里看到的一致。

### 🔧 本次修改文件

| 文件 | 改动 |
|---|---|
| `src/heliusWs.js` | 重写 `_extractTrade`(签名者做锚点、过滤多跳路由、SOL 金额只看签名者);`_subscribeBatch` 先建新后取消旧;防抖延长到 5s;CHUNK_SIZE 可配置 |
| `src/monitor.js` | 大额交易改 INFO 日志 |

### 新增 .env 配置

```bash
HELIUS_CHUNK_SIZE=50           # 每个批量订阅块包含多少币(默认 50)
HELIUS_BATCH_THRESHOLD=0       # 0=永远批量订阅(推荐,V3 已默认)
```

### 验证方法

启动后观察日志,大额交易(>1 SOL)会打印出来:

```bash
journalctl -u sol-rsi-monitor -f | grep HeliusTrade
```

对比 GMGN 上的最新几笔交易,应该:
- **方向正确**(BUY/SELL 和 GMGN 一致)
- **金额接近**(差异 < 10%,因为我们扣了手续费 + 签名者路径更严格)
- **不会出现"多跳中转"的虚假交易**(这种交易会被过滤)

---

## V3 修复 — Helius 订阅确认慢

详见前一版 CHANGELOG。核心:BATCH_THRESHOLD=0 强制批量订阅、修复防抖无限重置、加重试。

## V2 修复 — 量能虚高

详见前一版 CHANGELOG。修 WSOL `Math.abs` 累加放大。**V4 进一步更新了方向判断**。

## V1 修复 — Buy/Sell 显示 `-`

详见前一版 CHANGELOG。修前端/后端数据流。

---

## 部署

```bash
git pull
npm install --omit=dev
sudo systemctl restart sol-rsi-monitor

# 观察日志
journalctl -u sol-rsi-monitor -f | grep -E "HeliusTrade|HeliusWS|VolDiag"

# 订阅状态
curl -s http://localhost:3001/api/dashboard | jq '.heliusStats'
```

## 95 币场景推荐参数

```bash
# .env
VOL_WINDOW_SEC=300             # 5 分钟量能窗口
HELIUS_CHUNK_SIZE=50           # 50/块,95 币 = 2 块
HELIUS_BATCH_THRESHOLD=0       # 永远批量
MAX_MONITOR_TOKENS=95          # 最多监控数
OVERVIEW_PATROL_SEC=7200       # FDV/LP 巡检间隔
```

## 关于 LaserStream gRPC

Helius Business 计划含 LaserStream gRPC。优点:

- **无订阅槽位限制**(WS 有 ~50 隐性限制)
- **延迟更低**(gRPC < WebSocket)
- **吞吐更大**

本次修复后 WebSocket 方案对 95 币完全够用。如果未来监控 300+ 币,再考虑迁 gRPC。
