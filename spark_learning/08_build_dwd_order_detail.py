from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 项目路径
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

ODS_DIR = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ods"
)

DWD_OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dwd"
    / "dwd_order_detail"
)


# =========================================================
# 2. 创建Spark
# =========================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("BuildDwdOrderDetail")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# 3. 读取ODS表
# =========================================================

def read_ods_tables(spark):
    order_df = spark.read.parquet(
        str(ODS_DIR / "ods_order")
    )

    order_detail_df = spark.read.parquet(
        str(ODS_DIR / "ods_order_detail")
    )

    customer_df = spark.read.parquet(
        str(ODS_DIR / "ods_customer")
    )

    product_df = spark.read.parquet(
        str(ODS_DIR / "ods_product")
    )

    return (
        order_df,
        order_detail_df,
        customer_df,
        product_df,
    )


# =========================================================
# 4. 构建DWD宽表
# =========================================================

def build_dwd(
        order_df,
        order_detail_df,
        customer_df,
        product_df,
):
    # 给四张表起简称
    order = order_df.alias("o")
    detail = order_detail_df.alias("d")
    customer = customer_df.alias("c")
    product = product_df.alias("p")

    # 从订单明细出发进行关联
    joined_df = (
        detail

        # 通过order_id找到订单
        .join(
            order,
            F.col("d.order_id")
            == F.col("o.order_id"),
            "left",
        )

        # 通过customer_id找到客户
        .join(
            F.broadcast(customer),
            F.col("o.customer_id")
            == F.col("c.customer_id"),
            "left",
        )

        # 通过product_id找到商品
        .join(
            F.broadcast(product),
            F.col("d.product_id")
            == F.col("p.product_id"),
            "left",
        )
    )

    return joined_df


# =========================================================
# 5. 检查关联结果
# =========================================================

def check_join_result(
        order_detail_df,
        joined_df,
):
    source_rows = order_detail_df.count()

    result = (
        joined_df
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "joined_rows"
            ),

            F.countDistinct(
                F.col("d.order_detail_id")
            ).alias(
                "distinct_detail_rows"
            ),

            F.sum(
                F.when(
                    F.col("o.order_id").isNull(),
                    1,
                ).otherwise(0)
            ).alias(
                "missing_order_rows"
            ),

            F.sum(
                F.when(
                    F.col("c.customer_id").isNull(),
                    1,
                ).otherwise(0)
            ).alias(
                "missing_customer_rows"
            ),

            F.sum(
                F.when(
                    F.col("p.product_id").isNull(),
                    1,
                ).otherwise(0)
            ).alias(
                "missing_product_rows"
            ),
        )
        .first()
    )

    joined_rows = result["joined_rows"]
    distinct_detail_rows = result[
        "distinct_detail_rows"
    ]

    duplicate_rows = (
        joined_rows
        - distinct_detail_rows
    )

    missing_order_rows = (
        result["missing_order_rows"] or 0
    )

    missing_customer_rows = (
        result["missing_customer_rows"] or 0
    )

    missing_product_rows = (
        result["missing_product_rows"] or 0
    )

    print("\n========== DWD关联检查 ==========")
    print(f"ODS明细行数：{source_rows}")
    print(f"关联后行数：{joined_rows}")
    print(f"重复明细数量：{duplicate_rows}")
    print(f"找不到订单的明细：{missing_order_rows}")
    print(f"找不到客户的明细：{missing_customer_rows}")
    print(f"找不到商品的明细：{missing_product_rows}")

    if source_rows != joined_rows:
        raise ValueError(
            "关联前后行数不一致"
        )

    if duplicate_rows != 0:
        raise ValueError(
            "关联后出现重复订单明细"
        )

    if missing_order_rows != 0:
        raise ValueError(
            "存在找不到订单的明细"
        )

    if missing_customer_rows != 0:
        raise ValueError(
            "存在找不到客户的明细"
        )

    if missing_product_rows != 0:
        raise ValueError(
            "存在找不到商品的明细"
        )

    print("DWD关联检查全部通过")


# =========================================================
# 6. 选择DWD需要的字段
# =========================================================

def select_dwd_columns(joined_df):
    dwd_df = (
        joined_df
        .select(
            # 明细信息
            F.col(
                "d.order_detail_id"
            ).alias(
                "order_detail_id"
            ),

            F.col(
                "d.order_id"
            ).alias(
                "order_id"
            ),

            F.col(
                "d.product_id"
            ).alias(
                "product_id"
            ),

            F.col(
                "d.quantity"
            ).alias(
                "quantity"
            ),

            F.col(
                "d.unit_price"
            ).alias(
                "unit_price"
            ),

            F.col(
                "d.discount_amount"
            ).alias(
                "discount_amount"
            ),

            F.col(
                "d.actual_amount"
            ).alias(
                "actual_amount"
            ),

            F.col(
                "d.created_at"
            ).alias(
                "detail_created_at"
            ),

            # 订单信息
            F.col(
                "o.order_no"
            ).alias(
                "order_no"
            ),

            F.col(
                "o.customer_id"
            ).alias(
                "customer_id"
            ),

            F.col(
                "o.shop_id"
            ).alias(
                "shop_id"
            ),

            F.col(
                "o.order_time"
            ).alias(
                "order_time"
            ),

            F.col(
                "o.order_status"
            ).alias(
                "order_status"
            ),

            F.col(
                "o.order_amount"
            ).alias(
                "order_amount"
            ),

            F.col(
                "o.coupon_amount"
            ).alias(
                "coupon_amount"
            ),

            F.col(
                "o.freight_amount"
            ).alias(
                "freight_amount"
            ),

            F.col(
                "o.payable_amount"
            ).alias(
                "payable_amount"
            ),

            F.col(
                "o.order_channel"
            ).alias(
                "order_channel"
            ),

            F.col(
                "o.receiver_province_code"
            ).alias(
                "receiver_province_code"
            ),

            F.col(
                "o.receiver_province_name"
            ).alias(
                "receiver_province_name"
            ),

            F.col(
                "o.updated_at"
            ).alias(
                "order_updated_at"
            ),

            # 客户信息
            F.col(
                "c.customer_name"
            ).alias(
                "customer_name"
            ),

            F.col(
                "c.gender"
            ).alias(
                "gender"
            ),

            F.col(
                "c.age"
            ).alias(
                "age"
            ),

            F.col(
                "c.phone"
            ).alias(
                "phone"
            ),

            F.col(
                "c.email"
            ).alias(
                "email"
            ),

            F.col(
                "c.member_level"
            ).alias(
                "member_level"
            ),

            F.col(
                "c.customer_status"
            ).alias(
                "customer_status"
            ),

            F.col(
                "c.province_code"
            ).alias(
                "customer_province_code"
            ),

            F.col(
                "c.province_name"
            ).alias(
                "customer_province_name"
            ),

            F.col(
                "c.register_time"
            ).alias(
                "customer_register_time"
            ),

            # 商品信息
            F.col(
                "p.product_name"
            ).alias(
                "product_name"
            ),

            F.col(
                "p.category_id"
            ).alias(
                "category_id"
            ),

            F.col(
                "p.category_name"
            ).alias(
                "category_name"
            ),

            F.col(
                "p.brand_name"
            ).alias(
                "brand_name"
            ),

            F.col(
                "p.unit_price"
            ).alias(
                "product_standard_price"
            ),

            F.col(
                "p.cost_price"
            ).alias(
                "cost_price"
            ),

            F.col(
                "p.product_status"
            ).alias(
                "product_status"
            ),
        )

        # 增加本次加工信息
        .withColumn(
            "etl_time",
            F.current_timestamp(),
        )

        .withColumn(
            "source_system",
            F.lit("ecommerce"),
        )

        .withColumn(
            "etl_batch",
            F.date_format(
                F.current_timestamp(),
                "yyyyMMddHHmmss",
            ),
        )
    )

    return dwd_df


# =========================================================
# 7. 保存和回读检查
# =========================================================

def save_and_check(dwd_df):
    output_rows = dwd_df.count()

    print("\n========== DWD输出信息 ==========")
    print(f"DWD输出行数：{output_rows}")
    print(f"DWD输出目录：{DWD_OUTPUT_PATH}")

    (
        dwd_df
        .write
        .mode("overwrite")
        .parquet(
            str(DWD_OUTPUT_PATH)
        )
    )

    check_df = (
        dwd_df.sparkSession
        .read
        .parquet(
            str(DWD_OUTPUT_PATH)
        )
    )

    saved_rows = check_df.count()

    print(f"回读行数：{saved_rows}")

    if output_rows != saved_rows:
        raise ValueError(
            "DWD保存前后行数不一致"
        )

    print("DWD订单明细宽表保存成功")


# =========================================================
# 8. 主程序
# =========================================================

def main():
    spark = create_spark()

    try:
        print("开始构建Spark DWD订单明细宽表")

        (
            order_df,
            order_detail_df,
            customer_df,
            product_df,
        ) = read_ods_tables(spark)

        joined_df = build_dwd(
            order_df,
            order_detail_df,
            customer_df,
            product_df,
        )

        check_join_result(
            order_detail_df,
            joined_df,
        )

        dwd_df = select_dwd_columns(
            joined_df
        )

        print("\n========== DWD字段结构 ==========")
        dwd_df.printSchema()

        print("\n========== DWD示例数据 ==========")
        dwd_df.show(
            5,
            truncate=False,
        )

        save_and_check(dwd_df)

        print("\nSpark DWD构建全部完成")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
