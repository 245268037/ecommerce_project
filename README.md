
# 电商数据仓库与经营分析项目

基于Python、Pandas、PySpark、Linux Shell和Power BI构建的电商离线数据仓库与经营分析项目。

项目从订单、订单明细、客户和商品四类原始数据出发，分别使用Pandas和PySpark实现ODS、DWD、DWS、ADS分层加工，并完成数据质量检查、跨层指标核对、RFM用户分群、商品ABC分析、Power BI数据集构建和Linux自动化运行。

## 一、项目目标

本项目主要解决以下问题：

- 将多个电商业务CSV统一接入数据仓库；
- 对空值、重复、类型、金额和业务规则进行检查；
- 分别使用Pandas和PySpark实现分层数据加工；
- 建立ODS、DWD、DWS、ADS和BI数据模型；
- 使用Parquet与Snappy存储Spark数仓数据；
- 统一有效订单、用户、商品、地区和日期指标口径；
- 通过跨层核对防止订单、用户、销量和金额丢失；
- 完成RFM用户价值分群和商品ABC分析；
- 构建Power BI星型模型和经营分析看板；
- 使用Shell、cron和flock实现自动运行与重复任务控制；
- 练习Spark广播关联、Shuffle、缓存、分区、增量加载和小文件优化。

## 二、数据规模

| 数据对象 | 数据量 |
|---|---:|
| 订单 | 100,000 |
| 订单明细 | 299,831 |
| 客户 | 10,000 |
| 商品 | 3,000 |
| 有效销售订单 | 84,991 |
| 有效商品明细 | 255,008 |
| 有效商品销量 | 765,960 |
| RFM分析用户 | 9,998 |

有效销售状态统一定义为：

```text
已支付、已发货、已完成
```

取消、退款和待支付订单不进入有效销售指标。

## 三、技术栈

### 数据处理与数仓开发

- Python 3.13；
- Pandas、NumPy；
- PySpark 4.1.1、Spark SQL；
- OpenJDK 17；
- ODS、DWD、DWS、ADS分层建模。

### 数据存储与查询

- CSV；
- Parquet与Snappy压缩；
- SQLite；
- SQL聚合、窗口计算和多表关联。

### 数据质量与测试

- 自定义数据质量规则；
- 跨层数量与金额核对；
- Pytest；
- Spark数仓指标验收与性能体检。

### Linux与工程化

- Linux、Bash；
- cron定时调度；
- flock并发锁；
- 日志、状态码和信号处理；
- Git与GitHub。

### 数据分析与展示

- OpenPyXL；
- Power BI；
- RFM用户价值分析；
- 商品ABC分析。

## 四、数据仓库架构

项目包含Pandas和Spark两套数据处理流水线：

```text
                         ┌─ Pandas / NumPy
                         │      ↓
RAW原始CSV ── 数据质量检查 ─┤  ODS → DWD → DWS → ADS → BI数据集
                         │                              ↓
                         │                         Power BI看板
                         │
                         └─ PySpark / Spark SQL
                                ↓
                            明确字段类型
                                ↓
                         ODS → DWD → DWS → ADS
                                ↓
                         Parquet + Snappy
                                ↓
                       29项跨层验收与性能体检
```

### 各层主要职责

| 层级 | 主要职责 |
|---|---|
| RAW | 保存订单、订单明细、客户和商品原始CSV |
| ODS | 保留源数据并统一字段结构和技术字段 |
| DWD | 以订单明细为核心，关联订单、客户和商品信息 |
| DWS | 按用户、商品和地区形成主题汇总数据 |
| ADS | 生成每日销售、月度趋势、RFM和商品ABC指标 |
| BI | 构建事实表、维度表和Power BI分析模型 |

Pandas版主要输出CSV文件，Spark版主要输出Parquet文件。两套流水线使用相同的有效销售状态和指标统计口径。

## 五、数据质量体系

项目实现以下质量规则：

| 规则 | 检查内容 |
| --- | --- |
| NullRule | 必填字段空值 |
| TypeRule | 日期、数量和金额类型 |
| DuplicateRule | 主键和业务数据重复 |
| BusinessRule | 状态、时间和业务逻辑 |
| CountRule | 数据量异常 |
| AmountRule | 负数和金额异常 |

质量检查结果可导出：

- 质量报告；

- 异常明细；

- 质量评分；

- 历史质量记录。

## 六、指标口径

### 有效订单数

状态属于“已支付、已发货、已完成”的订单去重数量。

### 有效销售额

有效订单的应付金额合计。

### 有效客户数

至少存在一笔有效订单的客户去重数量。

### 客单价

```
有效销售额 ÷ 有效订单数
```

### 有效商品销量

有效订单明细中的商品数量合计。

### 商品销售额

有效订单明细中的实际金额合计。

订单销售额和商品销售额不要求完全相等：

```
订单应付金额 = 商品金额 - 优惠券金额 + 运费
```

因此项目分别在订单链路和商品链路内进行一致性核对。

## 七、跨层指标核对

ETL完成后自动核对：

- DWD有效订单数 → DWS用户订单数；

- DWD有效订单金额 → DWS用户金额；

- DWD有效订单数 → ADS日销售订单数；

- DWD有效订单金额 → ADS日销售金额；

- DWS用户数 → ADS用户数；

- DWS用户订单数 → ADS用户订单数；

- DWS用户金额 → ADS用户金额；

- DWD有效商品销量 → DWS商品销量；

- DWD有效商品金额 → DWS商品金额；

- DWS商品数 → ADS商品数；

- DWS商品销量 → ADS商品销量；

- DWS商品金额 → ADS商品金额。

任意指标核对失败，任务返回非0状态码。

## 八、RFM用户价值分析

以2025-12-31为分析基准日，计算：

- Recency：最近一次有效消费距基准日的天数；

- Frequency：有效订单数量；

- Monetary：有效订单应付金额。

根据R、F、M评分将用户划分为8类：

- 重要价值用户；

- 重要保持用户；

- 重要发展用户；

- 重要挽留用户；

- 一般价值用户；

- 一般保持用户；

- 一般发展用户；

- 一般挽留用户。

## 九、Linux运行保障

`run_etl.sh`提供以下能力：

- 自动定位项目绝对路径；

- 检查Python虚拟环境；

- 检查输入文件是否存在、可读、非空；

- 检查磁盘、内存、CPU和ETL进程；

- 使用flock防止任务重复执行；

- 记录Python子进程PID；

- 处理SIGINT和SIGTERM信号；

- 保存每次运行日志；

- 检查输出文件存在性、更新时间和数据行数；

- 执行数仓跨层业务指标核对。

状态码约定：

| 状态码 | 含义 |
| ---: | --- |
| 0 | 执行成功 |
| 1 | 执行失败 |
| 2 | 已有ETL任务运行，本次跳过 |
| 130 | 收到SIGINT |
| 143 | 收到SIGTERM |

## 十、项目运行

### 1. 创建虚拟环境

```
python3 -m venv .venv
source .venv/bin/activate
```

### 2. 安装依赖

```
python -m pip install -r requirements.txt
```

### 3. 生成练习数据

```
python generate_ecommerce_data.py
```

### 4. 执行ETL

```
./run_etl.sh
```

### 5. 运行测试

```
PYTHONPATH="$PWD/SRC" \
python -m pytest tests -v
```

### 6. 定时运行示例

每天凌晨2点30分执行：

```
30 2 * * * /项目绝对路径/run_etl.sh >> /项目绝对路径/logs/cron.log 2>&1
```

## 十一、项目结构

```text
ECommerce_Project/
├── SRC/
│   ├── checker/                 # 数据质量检查
│   ├── cleaner/                 # 数据清洗
│   ├── config/                  # 路径、字段和业务配置
│   ├── diagnostics/             # 数据问题诊断
│   ├── exporter/                # 质量报告导出
│   ├── pipeline/                # Pandas ETL主流程
│   ├── utils/                   # 日志、文件和任务工具
│   ├── warehouse/               # Pandas数仓构建程序
│   ├── check_warehouse_metrics.py
│   └── main.py
├── spark_learning/
│   ├── schemas.py               # Spark字段结构
│   ├── 02～06                   # RAW读取与ODS构建
│   ├── 07                       # ODS关系检查
│   ├── 08                       # DWD宽表构建
│   ├── 09～11                   # DWS主题构建
│   ├── 12～16                   # ADS指标构建
│   ├── 17                       # Spark数仓最终验收
│   ├── 18～27                   # Spark优化与增量处理练习
│   └── 28                       # Spark性能体检
├── sql_learning/
│   ├── load_sqlite.py           # SQL练习数据装载
│   ├── run_sql.py               # SQL文件执行工具
│   └── sql/                     # 颗粒度与聚合练习
├── scripts/
│   ├── check_etl_output.sh
│   ├── cleanup_logs.sh
│   └── system_health_check.sh
├── tests/
│   └── test_base_builder.py
├── generate_ecommerce_data.py
├── requirements.txt
├── run_etl.sh                   # Pandas ETL流水线
├── run_spark_warehouse.sh       # Spark数仓流水线
└── README.md
```

## 十二、项目成果

- 完成10万订单和299831条订单明细的数据加工；
- 建立Pandas版ODS、DWD、DWS、ADS和BI分层模型；
- 建立Spark版ODS、DWD、DWS和ADS分层数仓；
- 使用Parquet和Snappy保存Spark数仓数据；
- 统一订单、用户、商品、地区和日期等指标口径；
- 完成RFM用户价值分群和商品ABC分析；
- 完成Pandas数仓12项跨层指标核对；
- 完成Spark数仓29项检查，全部通过；
- 完成Spark广播关联、Shuffle、缓存、分区和小文件优化实践；
- 完成按月份增量加载、参数化处理和新旧数据合并练习；
- 完成SQL颗粒度、一对多关联和聚合下推练习；
- 构建Power BI经营分析模型；
- 实现Linux定时调度、系统检查、并发锁、日志和退出状态管理；
- 完成从数据接入、质量控制、数仓建模、指标验收到BI展示的完整链路。


## 十三、Spark分层数据仓库

在原有Python与Pandas数仓的基础上，增加PySpark版本的数据处理流水线，用于练习分布式数据加工、Parquet存储、任务调度和性能优化。

### 1. Spark运行环境

- PySpark 4.1.1；
- OpenJDK 17；
- 本地运行模式：`local[4]`；
- Driver内存：2GB；
- Shuffle分区数：8；
- 数据存储格式：Parquet；
- 压缩方式：Snappy。

### 2. Spark数仓分层

Spark数仓按照ODS、DWD、DWS和ADS四层建设：

- ODS层：读取订单、订单明细、客户和商品CSV，明确字段类型后保存为Parquet；
- DWD层：关联订单、订单明细、客户和商品，生成订单明细宽表；
- DWS层：生成用户销售、商品销售和地区销售主题数据；
- ADS层：生成每日销售、月度趋势、RFM用户分群和商品ABC分析结果。

Spark生成的数据保存在：

Spark生成的数据保存在：

```text
warehouse_spark/
├── ods/
├── dwd/
├── dws/
└── ads/
```

### 3. 一键执行Spark流水线

进入项目目录并激活虚拟环境：

```bash
cd ~/projects/ECommerce_Project
source .venv/bin/activate
```

执行完整Spark数仓流水线：

```bash
./run_spark_warehouse.sh
```

流水线会依次完成：

1. RAW数据读取检查；
2. 四张ODS表构建；
3. ODS表关系检查；
4. DWD订单明细宽表构建；
5. DWS用户、商品和地区主题构建；
6. ADS每日销售和月度趋势构建；
7. ADS用户RFM分群构建；
8. ADS商品ABC分析构建；
9. Spark数仓跨层指标验收。

### 4. 数据规模与验收结果

- 订单数据：100000行；
- 订单明细：299831行；
- 客户数据：10000行；
- 商品数据：3000行；
- DWS有效用户：9998人；
- DWS商品：3000个；
- ADS销售日期：364天；
- 最终验收检查项：29项；
- 验收结果：29项通过，0项失败。

### 5. Spark性能优化实践

项目中完成了以下Spark性能优化练习：

- 使用广播关联处理客户表和商品表，减少大表数据搬运；
- 通过执行计划识别`Exchange`和Shuffle；
- 对比`repartition()`与`coalesce()`的使用场景；
- 使用`cache()`、`persist()`和`unpersist()`管理重复使用的数据；
- 检查客户、商品、地区和渠道的数据倾斜情况；
- 对DWS和ADS小结果表使用`coalesce(1)`减少Parquet小文件；
- 使用`partitionBy()`按业务日期保存数据；
- 完成按月份增量加载、参数化处理和新旧数据合并示例；
- 编写Spark数仓性能体检脚本，检查数据量、分区数、文件数量和数据倾斜。

优化后，核心DWS和ADS小结果表均输出为单个Parquet数据文件，并重新通过全部29项跨层指标验收。

## 十四、后续优化

- 增加更多单元测试和异常场景测试；
- 将增量加载逻辑正式接入主流水线；
- 增加任务失败重试和断点续跑机制；
- 引入Hive Metastore管理Spark数据表；
- 引入Airflow进行任务编排；
- 引入Docker实现环境标准化；
- 增加数据血缘和元数据管理；
- 补充Power BI看板截图和PDF展示材料。
