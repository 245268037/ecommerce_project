from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ==============================
# 1.创建SparkSession
# ==============================

spark = (
    SparkSession.builder
    .appName("SparkRuntimeLearning")
    .getOrCreate()
)


# 减少不必要的日志输出
spark.sparkContext.setLogLevel(
    "WARN"
)


# ==============================
# 2.查看Spark运行环境
# ==============================

print(
    "\n========== Spark运行环境 =========="
)

print(
    f"Spark版本：{spark.version}"
)

print(
    f"应用名称："
    f"{spark.sparkContext.appName}"
)

print(
    f"Application ID："
    f"{spark.sparkContext.applicationId}"
)

print(
    f"运行模式："
    f"{spark.sparkContext.master}"
)

print(
    f"默认并行度："
    f"{spark.sparkContext.defaultParallelism}"
)

print(
    f"Shuffle分区数："
    f"{spark.conf.get('spark.sql.shuffle.partitions')}"
)


# ==============================
# 3.生成测试数据
# ==============================

number_df = spark.range(
    start=1,
    end=100001,
    step=1,
    numPartitions=4
)


print(
    "\n========== 原始DataFrame =========="
)

number_df.printSchema()

print(
    f"原始分区数："
    f"{number_df.rdd.getNumPartitions()}"
)

print(
    f"原始数据行数："
    f"{number_df.count()}"
)


# ==============================
# 4.执行转换
# ==============================

bucket_df = (
    number_df
    .withColumn(
        "bucket",
        F.col("id") % 4
    )
    .withColumn(
        "amount",
        F.col("id") * 10
    )
)


# ==============================
# 5.分组聚合
# ==============================

summary_df = (
    bucket_df
    .groupBy(
        "bucket"
    )
    .agg(
        F.count("*").alias(
            "row_count"
        ),
        F.sum("amount").alias(
            "total_amount"
        ),
        F.avg("amount").alias(
            "avg_amount"
        )
    )
    .orderBy(
        "bucket"
    )
)


print(
    "\n========== 执行计划 =========="
)

summary_df.explain(
    mode="formatted"
)


print(
    "\n========== 聚合结果 =========="
)

summary_df.show(
    truncate=False
)


# ==============================
# 6.关闭Spark
# ==============================

spark.stop()

print(
    "\nSpark应用执行完成"
)
