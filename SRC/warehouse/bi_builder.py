import pandas as pd

from warehouse.base_builder import BaseBuilder
from config.dwd_schema import (
    DWD_ORDER_DETAIL_SCHEMA
)
from config.settings import (
    VALID_SALES_STATUSES
)
from utils.logger import logger

class BIBuilder(BaseBuilder):


    def build_fact_tables(
            self,
            dwd_file,
            order_output,
            detail_output
    ):
        logger.info(
            "开始构建Power BI事实表"
        )

        # =====================
        # 1.读取DWD订单明细
        # =====================

        df = self.read(
            dwd_file,
            DWD_ORDER_DETAIL_SCHEMA
        )

        logger.info(
            f"DWD明细读取完成，"
            f"数据量={len(df)}"
        )

        # =====================
        # 2.检查必要字段
        # =====================

        order_columns = [
            "order_id",
            "order_no",
            "customer_id",
            "shop_id",
            "order_time",
            "order_status",
            "order_amount",
            "coupon_amount",
            "freight_amount",
            "payable_amount",
            "order_channel",
            "receiver_province_code",
            "receiver_province_name"
        ]

        detail_columns = [
            "order_detail_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_amount",
            "actual_amount",
            "created_at"
        ]

        required_columns = (
            order_columns
            +
            detail_columns
        )

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                "构建Power BI事实表缺少字段："
                f"{missing_columns}"
            )

        # =====================
        # 3.构建订单事实表
        # =====================

        fact_order = (
            df.drop_duplicates(
                subset=["order_id"]
            )[
                order_columns
            ]
            .copy()
        )

        fact_order["order_time"] = (
            pd.to_datetime(
                fact_order["order_time"],
                errors="coerce"
            )
        )

        invalid_time_count = (
            fact_order["order_time"]
            .isna()
            .sum()
        )

        if invalid_time_count > 0:
            raise ValueError(
                "订单事实表存在无效时间，"
                f"数量={invalid_time_count}"
            )

        fact_order["order_date"] = (
            fact_order["order_time"]
            .dt.strftime("%Y-%m-%d")
        )

        # 给Power BI准备统一销售标志
        fact_order["is_valid_sale"] = (
            fact_order["order_status"]
            .isin(
                VALID_SALES_STATUSES
            )
        )

        # =====================
        # 4.构建订单明细事实表
        # =====================

        fact_order_detail = (
            df[
                detail_columns
            ]
            .copy()
        )

        fact_order_detail["created_at"] = (
            pd.to_datetime(
                fact_order_detail[
                    "created_at"
                ],
                errors="coerce"
            )
        )

        # =====================
        # 5.检查事实表主键
        # =====================

        order_duplicate_count = (
            fact_order["order_id"]
            .duplicated()
            .sum()
        )

        detail_duplicate_count = (
            fact_order_detail[
                "order_detail_id"
            ]
            .duplicated()
            .sum()
        )

        if order_duplicate_count > 0:
            raise ValueError(
                "订单事实表存在重复订单，"
                f"数量={order_duplicate_count}"
            )

        if detail_duplicate_count > 0:
            raise ValueError(
                "订单明细事实表存在重复明细，"
                f"数量={detail_duplicate_count}"
            )

        # =====================
        # 6.检查订单与明细关系
        # =====================

        orphan_detail_count = (
            ~fact_order_detail[
                "order_id"
            ].isin(
                fact_order["order_id"]
            )
        ).sum()

        if orphan_detail_count > 0:
            raise ValueError(
                "订单明细事实表存在孤立订单，"
                f"数量={orphan_detail_count}"
            )

        # =====================
        # 7.检查销售口径
        # =====================

        total_order_count = len(
            fact_order
        )

        valid_order_count = (
            fact_order[
                "is_valid_sale"
            ].sum()
        )

        excluded_order_count = (
            total_order_count
            -
            valid_order_count
        )

        logger.info(
            f"Power BI订单事实表："
            f"全部订单={total_order_count}，"
            f"有效销售订单={valid_order_count}，"
            f"排除订单={excluded_order_count}"
        )

        # =====================
        # 8.检查金额
        # =====================

        for column in [
            "order_amount",
            "coupon_amount",
            "freight_amount",
            "payable_amount"
        ]:
            fact_order[column] = (
                pd.to_numeric(
                    fact_order[column],
                    errors="coerce"
                )
                .fillna(0)
            )

        for column in [
            "quantity",
            "unit_price",
            "discount_amount",
            "actual_amount"
        ]:
            fact_order_detail[column] = (
                pd.to_numeric(
                    fact_order_detail[
                        column
                    ],
                    errors="coerce"
                )
                .fillna(0)
            )

        valid_sales_amount = (
            fact_order.loc[
                fact_order[
                    "is_valid_sale"
                ],
                "payable_amount"
            ]
            .sum()
        )

        logger.info(
            f"Power BI有效销售金额："
            f"{valid_sales_amount:.2f}"
        )

        # =====================
        # 9.保存事实表
        # =====================

        self.save(
            fact_order,
            order_output
        )

        self.save(
            fact_order_detail,
            detail_output
        )

        logger.info(
            f"Power BI订单事实表完成："
            f"{order_output}，"
            f"数据量={len(fact_order)}"
        )

        logger.info(
            f"Power BI订单明细事实表完成："
            f"{detail_output}，"
            f"数据量={len(fact_order_detail)}"
        )

        return {
            "fact_order": fact_order,
            "fact_order_detail": (
                fact_order_detail
            )
        }

    def build_dimension_tables(
            self,
            ods_customer_file,
            ods_product_file,
            fact_order_file,
            fact_detail_file,
            customer_output,
            product_output,
            date_output
    ):
        logger.info(
            "开始构建Power BI维度表"
        )

        # =====================
        # 1.读取数据
        # =====================

        customer_df = self.read(
            ods_customer_file
        )

        product_df = self.read(
            ods_product_file
        )

        fact_order = self.read(
            fact_order_file
        )

        fact_detail = self.read(
            fact_detail_file
        )

        logger.info(
            f"维度数据读取完成："
            f"客户={len(customer_df)}，"
            f"商品={len(product_df)}，"
            f"订单={len(fact_order)}，"
            f"明细={len(fact_detail)}"
        )

        # =====================
        # 2.构建客户维度
        # =====================

        customer_required_columns = [
            "customer_id",
            "customer_name",
            "member_level"
        ]

        customer_optional_columns = [
            "gender",
            "age",
            "province_code",
            "province_name",
            "register_time",
            "customer_status"
        ]

        missing_customer_columns = [
            column
            for column in customer_required_columns
            if column not in customer_df.columns
        ]

        if missing_customer_columns:
            raise ValueError(
                "客户维度缺少必要字段："
                f"{missing_customer_columns}"
            )

        customer_columns = (
                customer_required_columns
                +
                [
                    column
                    for column
                    in customer_optional_columns
                    if column in customer_df.columns
                ]
        )

        dim_customer = (
            customer_df[
                customer_columns
            ]
            .drop_duplicates(
                subset=["customer_id"]
            )
            .copy()
        )

        if "register_time" in dim_customer.columns:
            dim_customer["register_time"] = (
                pd.to_datetime(
                    dim_customer["register_time"],
                    errors="coerce"
                )
            )

        # =====================
        # 3.构建商品维度
        # =====================

        product_required_columns = [
            "product_id",
            "product_name",
            "category_name"
        ]

        product_optional_columns = [
            "category_id",
            "brand_name",
            "cost_price",
            "product_status",
            "created_at"
        ]

        missing_product_columns = [
            column
            for column in product_required_columns
            if column not in product_df.columns
        ]

        if missing_product_columns:
            raise ValueError(
                "商品维度缺少必要字段："
                f"{missing_product_columns}"
            )

        product_columns = (
                product_required_columns
                +
                [
                    column
                    for column
                    in product_optional_columns
                    if column in product_df.columns
                ]
        )

        dim_product = (
            product_df[
                product_columns
            ]
            .drop_duplicates(
                subset=["product_id"]
            )
            .copy()
        )

        if "cost_price" in dim_product.columns:
            dim_product["cost_price"] = (
                pd.to_numeric(
                    dim_product["cost_price"],
                    errors="coerce"
                )
            )

        if "created_at" in dim_product.columns:
            dim_product["created_at"] = (
                pd.to_datetime(
                    dim_product["created_at"],
                    errors="coerce"
                )
            )

        # =====================
        # 4.构建日期维度
        # =====================

        if "order_date" not in fact_order.columns:
            raise ValueError(
                "订单事实表缺少order_date字段"
            )

        fact_order["order_date"] = (
            pd.to_datetime(
                fact_order["order_date"],
                errors="coerce"
            )
        )

        invalid_date_count = (
            fact_order["order_date"]
            .isna()
            .sum()
        )

        if invalid_date_count > 0:
            raise ValueError(
                "订单事实表存在无效日期，"
                f"数量={invalid_date_count}"
            )

        min_date = (
            fact_order["order_date"]
            .min()
            .normalize()
        )

        max_date = (
            fact_order["order_date"]
            .max()
            .normalize()
        )

        date_range = pd.date_range(
            start=min_date,
            end=max_date,
            freq="D"
        )

        dim_date = pd.DataFrame({
            "date": date_range
        })

        dim_date["year"] = (
            dim_date["date"].dt.year
        )

        dim_date["quarter"] = (
                "Q"
                +
                dim_date["date"]
                .dt.quarter
                .astype(str)
        )

        dim_date["month"] = (
            dim_date["date"].dt.month
        )

        dim_date["year_month"] = (
            dim_date["date"]
            .dt.strftime("%Y-%m")
        )

        dim_date["day"] = (
            dim_date["date"].dt.day
        )

        dim_date["week_of_year"] = (
            dim_date["date"]
            .dt.isocalendar()
            .week
            .astype(int)
        )

        dim_date["weekday_number"] = (
                dim_date["date"].dt.weekday + 1
        )

        weekday_mapping = {
            1: "星期一",
            2: "星期二",
            3: "星期三",
            4: "星期四",
            5: "星期五",
            6: "星期六",
            7: "星期日"
        }

        dim_date["weekday_name"] = (
            dim_date["weekday_number"]
            .map(weekday_mapping)
        )

        dim_date["is_weekend"] = (
            dim_date["weekday_number"]
            .isin([6, 7])
        )

        # =====================
        # 5.检查维度主键
        # =====================

        customer_duplicate_count = (
            dim_customer["customer_id"]
            .duplicated()
            .sum()
        )

        product_duplicate_count = (
            dim_product["product_id"]
            .duplicated()
            .sum()
        )

        date_duplicate_count = (
            dim_date["date"]
            .duplicated()
            .sum()
        )

        if customer_duplicate_count > 0:
            raise ValueError(
                "客户维度存在重复主键"
            )

        if product_duplicate_count > 0:
            raise ValueError(
                "商品维度存在重复主键"
            )

        if date_duplicate_count > 0:
            raise ValueError(
                "日期维度存在重复日期"
            )

        # =====================
        # 6.检查事实与维度关系
        # =====================

        missing_customer_count = (
            ~fact_order["customer_id"]
            .isin(
                dim_customer["customer_id"]
            )
        ).sum()

        missing_product_count = (
            ~fact_detail["product_id"]
            .isin(
                dim_product["product_id"]
            )
        ).sum()

        missing_date_count = (
            ~fact_order["order_date"]
            .isin(
                dim_date["date"]
            )
        ).sum()

        logger.info(
            f"Power BI维度关联检查："
            f"客户缺失={missing_customer_count}，"
            f"商品缺失={missing_product_count}，"
            f"日期缺失={missing_date_count}"
        )

        if missing_customer_count > 0:
            raise ValueError(
                "订单事实表存在无法匹配的客户"
            )

        if missing_product_count > 0:
            raise ValueError(
                "订单明细事实表存在无法匹配的商品"
            )

        if missing_date_count > 0:
            raise ValueError(
                "订单事实表存在无法匹配的日期"
            )

        # =====================
        # 7.保存维度表
        # =====================

        self.save(
            dim_customer,
            customer_output
        )

        self.save(
            dim_product,
            product_output
        )

        self.save(
            dim_date,
            date_output
        )

        logger.info(
            f"客户维度完成："
            f"{customer_output}，"
            f"数据量={len(dim_customer)}"
        )

        logger.info(
            f"商品维度完成："
            f"{product_output}，"
            f"数据量={len(dim_product)}"
        )

        logger.info(
            f"日期维度完成："
            f"{date_output}，"
            f"数据量={len(dim_date)}，"
            f"日期范围={min_date:%Y-%m-%d}"
            f"至{max_date:%Y-%m-%d}"
        )

        return {
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_date": dim_date
        }
