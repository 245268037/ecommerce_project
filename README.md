
# 电商数据仓库与经营分析项目

基于 Python、Pandas、Linux Shell 和 Power BI 构建的电商离线数据仓库项目。

项目从订单、订单明细、客户和商品四类原始数据出发，完成数据清洗、质量检查、ODS/DWD/DWS/ADS分层建模、跨层指标核对、RFM用户分群、Power BI数据集构建及Linux定时调度。

## 一、项目目标

本项目主要解决以下问题：

- 将多个电商业务CSV统一接入数据仓库；
- 对空值、重复、类型、金额和业务规则进行检查；
- 建立ODS、DWD、DWS、ADS分层数据模型；
- 统一有效销售订单、用户销售和商品销售统计口径；
- 通过跨层核对防止订单、用户、销量和金额丢失；
- 构建Power BI星型模型和经营分析看板；
- 使用Shell、cron和flock实现自动运行与重复任务控制。

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

- Python 3.13

- Pandas

- NumPy

- OpenPyXL

- Pytest

- Linux / Bash

- cron

- flock

- Git

- Power BI

## 四、数据仓库架构

```
RAW原始数据
    ↓
数据清洗与质量检查
    ↓
ODS原始数据层
    ↓
DWD订单明细事实层
    ↓
DWS用户、商品主题层
    ↓
ADS经营分析指标层
    ↓
BI事实表与维度表
    ↓
Power BI经营分析看板
```

### ODS原始数据层

ODS层保留清洗后的业务数据，并增加以下技术字段：

- `etl_time`：ETL处理时间；

- `source_system`：数据来源系统；

- `etl_batch`：ETL批次号。

### DWD明细数据层

以订单明细为核心，关联：

- 订单；

- 客户；

- 商品。

生成统一的订单明细宽表，并通过关联关系检查发现：

- 找不到订单的明细；

- 没有商品明细的订单；

- 找不到客户信息的订单；

- 找不到商品信息的明细；

- 重复订单和重复明细。

### DWS主题汇总层

构建以下主题：

- 用户销售主题；

- 商品销售主题；

- 地区销售主题。

用户主题在订单粒度统计，避免一笔订单包含多条商品明细时重复累计订单金额。

商品主题在订单明细粒度统计销量和商品实际金额。

### ADS应用指标层

构建：

- 日销售指标；

- 用户运营指标；

- 商品运营指标；

- RFM基础指标；

- RFM评分与用户分群；

- 商品销售排名与ABC分类。

### BI数据集

为Power BI构建星型模型：

事实表：

- `bi_fact_order.csv`

- `bi_fact_order_detail.csv`

维度表：

- `bi_dim_customer.csv`

- `bi_dim_product.csv`

- `bi_dim_date.csv`

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

```
ECommerce_Project/
├── SRC/
│   ├── checker/
│   ├── cleaner/
│   ├── config/
│   ├── diagnostics/
│   ├── exporter/
│   ├── pipeline/
│   ├── utils/
│   ├── warehouse/
│   ├── check_warehouse_metrics.py
│   └── main.py
├── scripts/
│   ├── check_etl_output.sh
│   ├── cleanup_logs.sh
│   └── system_health_check.sh
├── tests/
│   └── test_base_builder.py
├── generate_ecommerce_data.py
├── requirements.txt
├── run_etl.sh
└── README.md
```

## 十二、项目成果

- 完成10万订单和约30万订单明细的离线ETL处理；

- 建立ODS、DWD、DWS、ADS及BI分层模型；

- 统一订单、用户、商品和地区分析口径；

- 实现RFM用户价值分群和商品ABC分析；

- 实现12项跨层指标自动核对；

- 构建Power BI经营分析模型；

- 实现Linux定时调度、资源检查、并发锁和日志管理；

- 完成从数据接入、质量控制、数仓建模到BI展示的完整链路。

## 十三、后续优化

- 增加更多单元测试和异常场景测试；

- 将CSV存储升级为MySQL、Hive或Parquet；

- 增加增量抽取和断点续跑；

- 引入Airflow进行任务编排；

- 引入Docker实现环境标准化；

- 增加数据血缘和元数据管理；

- 补充Power BI看板截图和PDF展示材料。
