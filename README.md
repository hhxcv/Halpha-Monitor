# Halpha Monitor

稳定的价值取向、独立边界、第三方复用顺序和个人项目复杂度约束见 [项目原则](docs/PROJECT-PRINCIPLES.md)；AI 开发入口见 [AGENTS.md](AGENTS.md)。

这是一个独立的本地公开市场监控服务，不属于 Halpha App/Executor 产品运行时，也不取得账户、凭据或交易能力。“只读”指不会改变交易所或产品事实；页面允许修改本服务自己的 C2C 采集金额和支付方式。

## 本地隐私门禁

首次克隆后启用仓库自带的提交与推送门禁，并可随时扫描全部受 Git 管理或可能进入 Git 的文件：

```powershell
git config --local core.hooksPath .githooks
python .githooks/check_local_privacy.py --self-test
python .githooks/check_local_privacy.py --all
```

门禁命中时只显示类别与位置，不回显内容；不得使用 `--no-verify` 绕过。服务只为公开市场采集访问已声明来源，不包含遥测、错误自动上报或本地数据上传。

当前显式注册四个监控：

- Binance C2C 核算：比较公开 C2C 广告和现货一档；
- Binance USDⓈ-M 聪明钱：前向记录 BTCUSDT 的 30m/1h 网页内部 Smart Money 数据、官方 OI、标记价和资金费率。
- 山寨币异动雷达：对 Binance USDT 现货做全市场初筛，对有限候选补充闭合 5m K 线、BTC 相对表现、资金费率和 OI 变化，输出可解释的异动阶段与尾声风险；
- BTC 市场关联与相对强弱：基于 Binance Spot 闭合 UTC 日线，记录固定资产范围相对 BTC 的相关性、Beta、波动倍数和 7/30/90 日相对强弱。

## 运行

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m halpha_monitor
```

浏览器打开 `http://127.0.0.1:8790/`。服务只绑定本机回环地址。

页面会把当前选中的监控写入地址栏的 `monitor_id` 参数；刷新浏览器或复制该地址重新打开时，仍会进入同一监控。地址中的监控已不存在时，页面会移除失效参数并回到第一个已注册监控。

每项监控都可以在页面中独立开启或关闭，选择持久化到同一个 SQLite。关闭不会删除历史，也不会把后续空档记成故障；再次开启会立即请求一轮采集。C2C 和 Smart Money 默认开启；请求量较大的山寨币异动雷达和 BTC 关系监控默认关闭。

顶部只汇总运行事实：监控是否运行、当前采集负载、近 60 秒实际发出的公开 HTTP 请求数、计划采集轮次和最近完成时间。负载占用按各启用监控最近或当前一轮耗时除以各自采集周期后求和，低于 25% 为低、25% 至 74% 为中、75% 及以上为高；它属于公开网络采集活动的运行指标，不是 CPU 使用率或交易所请求权重。数据缺口不再汇总成笼统的“部分来源异常”；受影响的币种、范围或字段在对应表格内就地标记，未标记的展示值均为已通过校验的事实。

数据表保持表头与原生横向滚动条可见，全部列都可点击表头在正序和倒序之间切换；空值始终排在有值记录之后。非直观列的表头带有信息标记，鼠标悬停可查看计算或分类规则。页面的“历史范围”只控制下方历史曲线，支持 1/3/6/12 小时和 1/3/7/14/30 天，不会改变本轮评分。山寨币异动雷达的整行可在新标签页打开 Binance 行情：本轮确认存在同名 USDⓈ-M 合约时打开合约行情，否则明确回退到对应现货行情；该跳转只是用户发起的公开页面导航，不会读取账户或发送订单。

页面中的“采集条件”可以修改核算金额和支付方式；配置写入同一个 SQLite，监控已开启时保存后立即请求一轮新采集。默认核算金额为 `2000 CNY`，默认启用银行卡、支付宝和微信。

常用参数：

```powershell
.\.venv\Scripts\python.exe -m halpha_monitor `
  --interval-seconds 60 `
  --smart-money-interval-seconds 60 `
  --smart-money-jitter-seconds 5 `
  --smart-money-symbols BTCUSDT `
  --altcoin-radar-interval-seconds 300 `
  --altcoin-radar-jitter-seconds 30 `
  --altcoin-radar-min-quote-volume 5000000 `
  --altcoin-radar-max-candidates 30 `
  --altcoin-radar-workers 6 `
  --btc-relationship-interval-seconds 3600 `
  --btc-relationship-jitter-seconds 120 `
  --btc-relationship-workers 8 `
  --fiat CNY `
  --target-fiat 2000 `
  --pay-types BANK,ALIPAY,WECHAT `
  --assets USDT,USDC,BTC,ETH,BNB,SOL
```

数据库路径按 `--db-path`、`HALPHA_MONITOR_DB_PATH` 环境变量、`%LOCALAPPDATA%\Halpha\monitor\monitor.sqlite3` 的优先级选择。为避免可增长数据占用系统盘，可把用户级环境变量指向 Git 仓库外的非系统盘目录；实际路径只保存在本机，不写入仓库：

```powershell
$databasePath = Read-Host "请输入仓库外数据库绝对路径"
[Environment]::SetEnvironmentVariable(
  "HALPHA_MONITOR_DB_PATH",
  $databasePath,
  "User"
)
```

重新启动终端或服务后生效。默认保留 90 天，可用 `--retention-days` 调整。

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
- 某币种在当前金额和支付方式下没有匹配广告，是本轮查询的明确空结果而不是网络异常；页面会在核算表中列出该币种，不把其他已取得报价降级为“部分来源异常”；
- C2C 广告、现货一档和页面刷新不是原子快照，结果只适合观察与比较，不是订单价格或执行依据；
- 接口依据：[Binance P2P Skill 的公开查询契约](https://github.com/binance/binance-skills-hub/blob/main/skills/binance/p2p/SKILL.md) 与 [Binance Spot REST API](https://developers.binance.com/en/docs/products/spot/rest-api)，核对于 2026-07-25。

### USDⓈ-M 聪明钱

- Smart Money 来自 Binance 网页实际使用的 `/bapi/futures/v1/public/future/smart-money/...` 接口；它们未出现在正式 Developer API 文档中，没有稳定性、速率限制或变更承诺；
- 每个合约每轮读取一次仓位总览、官方 OI 和 premium index，并分别读取 30m/1h 分项与按时间倒序的第一条明细；所有请求均未登录、无 Cookie、无 API Key；
- `longPositions`/`shortPositions` 用于计算资金流失衡，官方 OI 与标记价用于计算 `净流 / OI`；巨鲸是全部交易员的子集，页面先扣除巨鲸后再计算非巨鲸失衡和巨鲸分歧；
- 当前研究结论固定为 `INSUFFICIENT_EVIDENCE`。表格中的净流方向、失衡和分歧只是研究特征，不是 B/S 建议、跟单信号或自动交易输入；
- 仓位总览的 `updateTime` 单独检查。总览陈旧时，该轮资金流仍可记录，但总览字段不进入特征和主表，受影响字段在原位置标记，详细原因同时保留在采集诊断中；
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

### 山寨币异动雷达

- 现货来源是 Binance 官方公开 Spot `exchangeInfo`、`ticker/24hr`、滚动窗口 `ticker` 和 `klines`；合约补充来源是官方公开 USDⓈ-M `premiumIndex` 与 `openInterestHist`。所有请求均未登录、无 Cookie、无 API Key，只读取公开市场数据；接口依据核对于 2026-08-06；
- “全市场”指每轮先读取当前可交易的 USDT 现货，并排除 BTC、内置稳定币名单和已知杠杆代币名单。所有剩余币都会经过 24h 流动性门槛；达到门槛的币再分批读取 1h 滚动统计，最后只对优先级最高的有限候选读取 5m K 线。扫描覆盖是整轮采集元数据，页面只在候选表上方显示一次本轮全市场数量、达到门槛数量和详查数量，不在每个币种行内重复；不能把候选详查误读为全币种深度分析；
- 默认 24h 成交额门槛为 `5,000,000 USDT`，详查最多 30 个候选，基础周期 300 秒并附加最多 30 秒抖动。滚动行情每批最多 100 个币，候选并发最多 6 个；这些边界可由上面的 CLI 参数调整，但提高范围前应评估 Binance 当前请求权重与同机其他监控占用；
- 评分只使用连续、已闭合且仍在有效截止点内的 5m K 线。主要特征包括 15m/1h 涨跌、最近 15m 相对之前 45m 的成交额与成交笔数放大、主动买入成交额占比、前高突破、区间位置、波动压缩和相对 BTC 的 15m 表现；存在同名 USDⓈ-M 合约时，再补充资金费率和 15m OI 变化；
- 页面明确展示时效口径：每根闭合 5m K 重算，短线主判看 15m，趋势背景看 1h，波动压缩比较覆盖约 2 小时 15 分，流动性与部分风险条件看 24h；“结论有效至”为市场截止后 15 分钟，到期后必须等待新采集。同轮全部候选的有效时间或市场截止一致时，页面在表格上方只显示一次；存在差异或缺失时才保留逐行列。“复核节奏”只是基于 5m 更新频率的风险检查要求，不是期望盈利持仓期；当前没有包含手续费、滑点的样本外回测，因此不会输出虚构的持仓时长；
- 阶段为“蓄势观察、启动、加速、尾声风险、回落确认、尚未形成”。`异动强度`、内部蓄势/启动分和 `尾声风险` 都是 `0–100` 的规则化异动分，不是证据充分性或经过样本外校准的概率，也不能证明存在操纵主体、保证后续上涨或构成买卖建议。页面不再用这些分数派生“高/中/低证据”标签；“关键事实”只列出对本轮规则归因贡献最大的已观测市场事实，不代表独立来源佐证或新闻事实；
- 每轮候选快照都继续写入 `monitor_sample`。当“蓄势观察、启动、加速、尾声风险、回落确认”首次出现、阶段发生变化，或同阶段持续 4 小时仍未建立新样本时，系统会在结果未知时冻结信号截止价，并建立 15 分钟、1 小时、4 小时三个后续检验任务。到期后即使币种已经离开候选列表，也会用该币与 BTC 同期连续闭合 5m K 线计算绝对收益、相对 BTC 收益、沿原判断方向的最大顺向波动和反向最大逆向波动；不插值、不用当前价格替代缺失历史；
- 后续检验把蓄势/启动/加速视为向上判断，把尾声风险/回落确认视为向下判断。只有绝对方向一致且相对 BTC 超过暂定的 `±0.5` 个百分点噪声带才记为“方向一致”，反向越过噪声带记为“方向相反”，其余为“未形成显著方向”。该口径不含手续费、滑点和可成交性，不能当作收益回测；页面先展示到期覆盖率作为防选择偏差护栏，每个“阶段 × 期限”至少完成 30 例后才显示方向一致率，否则明确显示样本积累中；
- 检验任务、完成结果和无法取得连续行情的状态均保存在同一个 SQLite，并与信号来源运行记录绑定，随现有保留期级联清理。检验补采按每轮最多 12 个到期任务执行，失败不会覆盖当前行情结果，也不会另起服务或无限增加线程；
- 单个候选 K 线或 OI 失败时，该候选字段保持为空并在原位置说明原因，详细原因同时保留在采集诊断中，其他候选结果仍照常展示；全市场基础行情失败、陈旧或限流时不生成新评分。HTTP 418/429 遵守 `Retry-After` 并打开共享有界退避，不用上一轮数据冒充当前结果；
- 首版明确不把链上数据混入市场评分。跨链地址、原生币与代币合约映射、区块确认时间和交易所地址标签口径不一致；在没有指定链、合约地址和可核验公开来源前，合并成一个“链上活跃度”会制造伪精度。后续应按具体链和币种建立独立事实，再验证其是否改善阶段识别；
- 官方依据：[Binance Spot REST API](https://developers.binance.com/en/docs/products/spot/rest-api)、[Binance Spot API 公开契约](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md) 与 [Binance USDⓈ-M Futures API](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/Introduction)。

### BTC 市场关联与相对强弱

- 资产范围固定为内置的 2026-07-21 研究快照：411 个 Binance Spot USDT 原生加密/锚定资产（含 BTC 参考，410 个比较对象）；运行时不依赖或改写 Research 仓库；
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
- 页面左侧用绿色“监控中 / 采集中”和灰色“已关闭”表达调度状态；数据区域另行区分最新结果、上一轮结果与历史快照。页面只读取已提交且通过校验的记录；最新采集失败时仍可显示上一次已校验样本及其截止时间；
- 历史曲线按真实采集时间分段，不跨越长于采集周期容忍阈值的空档；空档标记为“未采集时段”，不会拟合、插值或用领域默认值连接；
- 缺失或异常字段显示为空值符号并在原位置说明原因，不以假数据、拟合值或领域默认值补齐；近期失败明细默认收在次级“采集诊断”中；
- 没有消息队列、动态插件发现、前端构建、图表依赖、第二数据库或通用告警平台；
- 服务不会被 Halpha 产品代码导入、启动或停止，也不修改当前 L4 产品事实。

SQLite 样本使用关系型运行/索引字段，并在同一事务中保存每个监控的紧凑 payload；它不是 JSONL 文件，也不依赖逐行扫描查询。

## 注册新监控

1. 在 `src/halpha_monitor/monitors/` 新建独立模块，实现 `RegisteredMonitor` 的字段和 `collect()`。
2. `collect()` 返回 `CollectionBatch`；通过校验的样本放入 `samples`，局部失败放入带 scope 的 `issues`。
3. 只在 `monitors/__init__.py` 增加该监控的 CLI、settings 构造和显式注册；服务入口不再理解各监控参数。
4. 为采集逻辑、部分失败和页面投影增加对应测试，并让 built-in 契约测试覆盖新注册对象。

注册对象只声明采集间隔、筛选项、表列和主数值序列。共享进程不理解各监控的业务公式，页面也没有监控专属组件。实现 `ConfigurableMonitor` 时，配置读写必须原子化，`collect()` 每一轮只使用一个不可变配置快照，避免页面保存与采集线程拼接两套条件。需要保存外部原始证据时，在同一个 `CollectionBatch` 中附加 `CollectionArtifact`，不要另建数据库。大范围官方日线如需增量复用，只保留可删除重取的规范化缓存，不能把缓存冒充当前样本或研究证据。
