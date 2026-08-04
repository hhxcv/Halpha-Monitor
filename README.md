# Halpha Monitor

这是一个独立的本地公开市场监控服务，不属于 Halpha App/Executor 产品运行时，也不取得账户、凭据或交易能力。“只读”指不会改变交易所或产品事实；页面允许修改本服务自己的 C2C 采集金额和支付方式。

当前显式注册三个监控：

- Binance C2C 核算：比较公开 C2C 广告和现货一档；
- Binance USDⓈ-M 聪明钱：前向记录 BTCUSDT 的 30m/1h 网页内部 Smart Money 数据、官方 OI、标记价和资金费率。
- BTC 市场关联与相对强弱：基于 Binance Spot 闭合 UTC 日线，记录固定资产范围相对 BTC 的相关性、Beta、波动倍数和 7/30/90 日相对强弱。

## 运行

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m halpha_monitor
```

浏览器打开 `http://127.0.0.1:8790/`。服务只绑定本机回环地址。

每项监控都可以在页面中独立开启或关闭，选择持久化到同一个 SQLite。关闭不会删除历史，也不会把后续空档记成故障；再次开启会立即请求一轮采集。原有 C2C 和 Smart Money 首次升级保持默认开启，新增的 BTC 关系监控默认关闭。

页面中的“采集条件”可以修改核算金额和支付方式；配置写入同一个 SQLite，监控已开启时保存后立即请求一轮新采集。默认核算金额为 `2000 CNY`，默认启用银行卡、支付宝和微信。

常用参数：

```powershell
.\.venv\Scripts\python.exe -m halpha_monitor `
  --interval-seconds 60 `
  --smart-money-interval-seconds 60 `
  --smart-money-jitter-seconds 5 `
  --smart-money-symbols BTCUSDT `
  --btc-relationship-interval-seconds 3600 `
  --btc-relationship-jitter-seconds 120 `
  --btc-relationship-workers 8 `
  --fiat CNY `
  --target-fiat 2000 `
  --pay-types BANK,ALIPAY,WECHAT `
  --assets USDT,USDC,BTC,ETH,BNB,SOL
```

默认数据库为 `%LOCALAPPDATA%\Halpha\monitor\monitor.sqlite3`，可用 `--db-path` 覆盖。默认保留 90 天，可用 `--retention-days` 调整。

BTC 关系监控的规范化闭合日线缓存位于数据库同目录下的 `cache/btc-relationship/`。它只用于避免同一闭合日重复下载，不是第二数据库或研究证据；来源落后于当前应有闭合日时不会用于生成新指标。

未来增加合约不需要修改采集或页面代码，例如：

```powershell
.\.venv\Scripts\python.exe -m halpha_monitor `
  --smart-money-symbols BTCUSDT,ETHUSDT,SOLUSDT
```

## 数据含义

### C2C 核算

- C2C 广告来自 Binance 官方公开 Agent API 的 `ad-list` 与 `trade-methods`，不使用账户 API Key；
- 现货一档来自 Binance 官方公开 Spot `bookTicker`；
- 页面只在 API 返回的最多 20 条广告中筛选满足核算金额、支付方式、限额和库存的样本，“最佳”不代表全市场最优，也不构成可成交承诺；
- C2C 广告、现货一档和页面刷新不是原子快照，结果只适合观察与比较，不是订单价格或执行依据；
- 接口依据：[Binance P2P Skill 的公开查询契约](https://github.com/binance/binance-skills-hub/blob/main/skills/binance/p2p/SKILL.md) 与 [Binance Spot REST API](https://developers.binance.com/en/docs/products/spot/rest-api)，核对于 2026-07-25。

### USDⓈ-M 聪明钱

- Smart Money 来自 Binance 网页实际使用的 `/bapi/futures/v1/public/future/smart-money/...` 接口；它们未出现在正式 Developer API 文档中，没有稳定性、速率限制或变更承诺；
- 每个合约每轮读取一次仓位总览、官方 OI 和 premium index，并分别读取 30m/1h 分项与按时间倒序的第一条明细；所有请求均未登录、无 Cookie、无 API Key；
- `longPositions`/`shortPositions` 用于计算资金流失衡，官方 OI 与标记价用于计算 `净流 / OI`；巨鲸是全部交易员的子集，页面先扣除巨鲸后再计算非巨鲸失衡和巨鲸分歧；
- 当前研究结论固定为 `INSUFFICIENT_EVIDENCE`。表格中的净流方向、失衡和分歧只是研究特征，不是 B/S 建议、跟单信号或自动交易输入；
- 仓位总览的 `updateTime` 单独检查。总览陈旧时，该轮资金流仍可记录，但总览字段不进入特征和主表，该来源异常只在数据状态说明与采集诊断中保留；
- 分项字段契约变化、业务码异常、非零流量却没有可核对的最新明细、官方 OI/标记价陈旧、HTTP 418/429 或空响应时，不生成该范围的新特征；418/429 使用带抖动的指数退避；
- 接口与字段于 2026-07-27 再次直接核对。网页内部接口可读不代表它已经成为受支持的正式 API。

每个请求原子保存以下诊断字段和未经改写的有界响应正文：

```text
request_started_at
response_completed_at
http_status
business_code
schema_hash
response_sha256
record_count
response_body
```

这些原始响应与同轮派生样本、问题记录共享 `run_id`，随运行记录一起按保留期级联删除。默认 BTC、60 秒采集和 90 天保留会产生持续增长的本地 SQLite；增加合约前应相应评估磁盘空间或缩短保留期。

### BTC 市场关联与相对强弱

- 资产范围固定为 2026-07-21 研究快照中 411 个 Binance Spot USDT 原生加密/锚定资产（含 BTC 参考，410 个比较对象）；本能力不会刷新或改写 `research/market-universe/`；
- 数据来自 Binance 官方公开 Spot `1d` K 线，只使用已闭合的 UTC 日线；同一闭合日已经缓存时不会重复请求，新的闭合日出现后才增量补取；
- 主窗口为最近 365 个连续对齐日收益，最低需要 120 个观测；计算 Pearson、Spearman、BTC Beta、R²、波动倍数，以及相对 BTC 的 7/30/90 日累计强弱；
- 监控服务使用已锁定的 pandas/numpy，不复制研究侧 HAC 推断、FDR、多来源交叉核对和证据快照平台；页面的“强/中等/弱”仅指 Pearson 绝对值区间，不表示统计显著、预测或可交易；
- 资产样本不足时保留该资产行但指标为空并说明观测数量；来源未取得当前闭合日时指标同样为空，不沿用陈旧日线生成新指标；
- 原研究结论仍为 `SUPPORTS_WITHIN_SCOPE`，只支持既定样本内的关联描述；不支持因果、领先关系、未来预测、交易信号或 Alpha。

## 低复杂度边界

- 一个操作系统进程、一个 FastAPI 页面、一个 SQLite 数据库；BTC 关系监控只增加同目录下可删除重取的规范化日线缓存；
- SQLite 使用 WAL、外键和短事务；一次采集的运行、原始响应、样本和问题原子提交，全库保留期清理最多每小时执行一次；
- 每个注册监控使用一个线程，超时与异常不阻塞其他监控；
- Smart Money 的 60 秒基础间隔增加至多 5 秒随机抖动，418/429 退避只停止该监控的外部请求；
- 页面只读取已提交且通过校验的记录，并明确区分当前数据、上一轮数据和已过期历史；最新采集失败时仍可显示上一次可用样本及其截止时间；
- 历史曲线按真实采集时间分段，不跨越长于采集周期容忍阈值的空档；空档标记为“未采集时段”，不会拟合、插值或用领域默认值连接；
- 缺失或异常字段显示为空值符号并在原位置说明原因，不以假数据、拟合值或领域默认值补齐；近期失败明细默认收在次级“采集诊断”中；
- 没有消息队列、动态插件发现、前端构建、图表依赖、第二数据库或通用告警平台；
- 服务不会被 Halpha 产品代码导入、启动或停止，也不修改当前 L4 产品事实。

SQLite 样本使用关系型运行/索引字段，并在同一事务中保存每个监控的紧凑 payload；它不是 JSONL 文件，也不依赖逐行扫描查询。

## 注册新监控

1. 在 `src/halpha_monitor/monitors/` 新建独立模块，实现 `RegisteredMonitor` 的字段和 `collect()`。
2. `collect()` 返回 `CollectionBatch`；可用样本放入 `samples`，局部失败放入带 scope 的 `issues`。
3. 只在 `monitors/__init__.py` 增加该监控的 CLI、settings 构造和显式注册；服务入口不再理解各监控参数。
4. 为采集逻辑、部分失败和页面投影增加对应测试，并让 built-in 契约测试覆盖新注册对象。

注册对象只声明采集间隔、筛选项、表列和主数值序列。共享进程不理解各监控的业务公式，页面也没有监控专属组件。实现 `ConfigurableMonitor` 时，配置读写必须原子化，`collect()` 每一轮只使用一个不可变配置快照，避免页面保存与采集线程拼接两套条件。需要保存外部原始证据时，在同一个 `CollectionBatch` 中附加 `CollectionArtifact`，不要另建数据库。大范围官方日线如需增量复用，只保留可删除重取的规范化缓存，不能把缓存冒充当前样本或研究证据。
