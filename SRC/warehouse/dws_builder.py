import pandas as pd
from warehouse.base_builder import BaseBuilder
from config.dwd_schema import (
    DWD_ORDER_DETAIL_SCHEMA
)
from utils.logger import logger
from config.settings import VALID_SALES_STATUSES


class DWSbuilder(BaseBuilder):

    def build_user_sales(
            self,
            dwd_file,
            output_file
    ):
        logger.info(
            "开始构DWS建用户销售主题"
        )
        df = self.read(
            dwd_file,
            DWD_ORDER_DETAIL_SCHEMA
        )

        df["order_time"] = pd.to_datetime(
            df["order_time"],
            errors="coerce"
        )

        # =====================
        # 过滤有效销售订单
        # =====================

        all_order_count = (
            df["order_id"].nunique()
        )

        df = df[
            df["order_status"].isin(
                VALID_SALES_STATUSES
            )
        ].copy()

        valid_order_count = (
            df["order_id"].nunique()
        )

        logger.info(
            f"用户主题销售口径过滤："
            f"全部订单数={all_order_count}，"
            f"有效订单数={valid_order_count}，"
            f"排除订单数="
            f"{all_order_count - valid_order_count}"
        )


        # =====================
        # 用户销售主题
        # =====================

        order_level_df = (
            df.drop_duplicates(
                subset=["order_id"]
            )
            .copy()
        )

        logger.info(
            f"DWD明细行数：{len(df)}，"
            f"去重后订单数：{len(order_level_df)}"
        )

        # 客户维度缺失时，保留该订单，不让groupby将其丢弃。
        order_level_df["customer_name"] = (
            order_level_df["customer_name"]
            .fillna("未知客户")
        )

        user_sales = (
            order_level_df
            .groupby(
                [
                    "customer_id",
                    "customer_name"
                ],
                dropna=False
            )
            .agg(
                order_count=(
                    "order_id",
                    "nunique"
                ),
                total_amount=(
                    "payable_amount",
                    "sum"
                ),
                first_order_time=(
                    "order_time",
                    "min"
                ),
                last_order_time=(
                    "order_time",
                    "max"
                )
            )
            .reset_index()
        )

        user_sales["avg_amount"] = (
                user_sales["total_amount"]
                /
                user_sales["order_count"]
        ).round(2)

        source_order_count = (
            df["order_id"].nunique()
        )

        dws_order_count = (
            user_sales["order_count"].sum()
        )

        source_total_amount = (
            order_level_df["payable_amount"].sum()
        )

        dws_total_amount = (
            user_sales["total_amount"].sum()
        )

        logger.info(
            f"用户主题订单核对："
            f"源订单数={source_order_count}，"
            f"DWS订单数={dws_order_count}"
        )

        logger.info(
            f"用户主题金额核对："
            f"源订单金额={source_total_amount:.2f}，"
            f"DWS订单金额={dws_total_amount:.2f}"
        )

        if source_order_count != dws_order_count:
            raise ValueError(
                "用户销售主题订单数量核对失败："
                f"源订单数={source_order_count}，"
                f"DWS订单数={dws_order_count}"
            )

        amount_difference = abs(
            source_total_amount
            - dws_total_amount
        )

        if amount_difference > 0.01:
            raise ValueError(
                "用户销售主题金额核对失败："
                f"金额差异={amount_difference:.2f}"
            )
        self.save(
            user_sales,
            output_file
        )
        logger.info(
            "用户销售主题完成"
        )

        return user_sales

    def build_product_sales(
            self,
            dwd_file,
            output_file
    ):
        logger.info(
            "开始构建DWS商品销售主题"
        )

        # =====================
        # 1.读取DWD商品明细
        # =====================

        df = self.read(
            dwd_file,
            DWD_ORDER_DETAIL_SCHEMA
        )

        logger.info(
            f"DWD商品明细读取完成，"
            f"数据量：{len(df)}"
        )

        # =====================
        # 过滤有效销售明细
        # =====================

        all_detail_count = len(df)

        df = df[
            df["order_status"].isin(
                VALID_SALES_STATUSES
            )
        ].copy()

        valid_detail_count = len(df)

        logger.info(
            f"商品主题销售口径过滤："
            f"全部明细数={all_detail_count}，"
            f"有效明细数={valid_detail_count}，"
            f"排除明细数="
            f"{all_detail_count - valid_detail_count}"
        )

        # =====================
        # 2.检查商品维度缺失
        # =====================

        missing_product_count = (
                df["product_name"].isna()
                |
                df["category_name"].isna()
        ).sum()

        logger.info(
            f"商品维度缺失明细数："
            f"{missing_product_count}"
        )

        # =====================
        # 3.填充未知维度
        # =====================

        df["product_name"] = (
            df["product_name"]
            .fillna("未知商品")
        )

        df["category_name"] = (
            df["category_name"]
            .fillna("未知分类")
        )

        # 如果商品编号本身为空，也保留这部分销售事实
        df["product_id"] = (
            df["product_id"]
            .fillna("UNKNOWN_PRODUCT")
        )

        # =====================
        # 4.按商品聚合
        # =====================

        product_sales = (
            df.groupby(
                [
                    "product_id",
                    "product_name",
                    "category_name"
                ],
                dropna=False
            )
            .agg(
                sales_count=(
                    "quantity",
                    "sum"
                ),
                sales_amount=(
                    "actual_amount",
                    "sum"
                )
            )
            .reset_index()
        )

        # 保留两位小数
        product_sales["sales_amount"] = (
            product_sales["sales_amount"]
            .round(2)
        )

        # =====================
        # 5.核对销量
        # =====================

        source_sales_count = (
            df["quantity"].sum()
        )

        dws_sales_count = (
            product_sales["sales_count"].sum()
        )

        logger.info(
            f"商品主题销量核对："
            f"DWD销量={source_sales_count}，"
            f"DWS销量={dws_sales_count}"
        )

        if source_sales_count != dws_sales_count:
            raise ValueError(
                "商品主题销量核对失败："
                f"DWD销量={source_sales_count}，"
                f"DWS销量={dws_sales_count}"
            )

        # =====================
        # 6.核对销售额
        # =====================

        source_sales_amount = (
            df["actual_amount"].sum()
        )

        dws_sales_amount = (
            product_sales["sales_amount"].sum()
        )

        amount_difference = abs(
            source_sales_amount
            - dws_sales_amount
        )

        logger.info(
            f"商品主题金额核对："
            f"DWD金额={source_sales_amount:.2f}，"
            f"DWS金额={dws_sales_amount:.2f}"
        )

        if amount_difference > 0.01:
            raise ValueError(
                "商品主题金额核对失败："
                f"金额差异={amount_difference:.2f}"
            )

        # =====================
        # 7.保存DWS
        # =====================

        self.save(
            product_sales,
            output_file
        )

        logger.info(
            f"商品销售主题完成："
            f"{output_file}，"
            f"商品数={len(product_sales)}"
        )

        return product_sales
