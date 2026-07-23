from warehouse.base_builder import BaseBuilder
from config.settings import TECH_COLUMNS
from config.ods_schema import (
    ODS_ORDER_SCHEMA,
    ODS_ORDER_DETAIL_SCHEMA,
    ODS_CUSTOMER_SCHEMA,
    ODS_PRODUCT_SCHEMA
)
from config.dwd_schema import DWD_ORDER_COLUMNS
from utils.logger import logger

class DWDBuilder(BaseBuilder):

    def build_order_detail(
            self,
            order_file,
            order_detail_file,
            customer_file,
            product_file,
            output_file
    ):
        logger.info(
            "开始构建DWD"
        )
        # 读取ODS数据

        order_df = self.read(
            order_file,
            ODS_ORDER_SCHEMA
        )

        order_detail_df = self.read(
            order_detail_file,
            ODS_ORDER_DETAIL_SCHEMA
        )

        customer_df = self.read(
            customer_file,
            ODS_CUSTOMER_SCHEMA
        )

        product_df = self.read(

            product_file,

            ODS_PRODUCT_SCHEMA

        )
        logger.info(
            "ODS读取完成"
        )

        # ======================
        # 2.删除技术字段
        # ======================
        for df in [
            order_df,
            order_detail_df,
            customer_df,
            product_df
        ]:
            df.drop(
                columns=TECH_COLUMNS,
                errors="ignore",
                inplace=True
            )

        logger.info(
            f"订单数据量：{len(order_df)}，"
            f"订单数：{order_df['order_id'].nunique()}"
        )

        logger.info(
            f"订单明细数据量：{len(order_detail_df)}，"
            f"涉及订单数：{order_detail_df['order_id'].nunique()}"
        )

        logger.info(
            f"客户数据量：{len(customer_df)}，"
            f"客户数：{customer_df['customer_id'].nunique()}"
        )

        logger.info(
            f"商品数据量：{len(product_df)}，"
            f"商品数：{product_df['product_id'].nunique()}"
        )
        orphan_detail_count = (
            ~order_detail_df["order_id"].isin(
                order_df["order_id"]
            )
        ).sum()

        missing_detail_order_count = (
            ~order_df["order_id"].isin(
                order_detail_df["order_id"]
            )
        ).sum()

        missing_customer_count = (
            ~order_df["customer_id"].isin(
                customer_df["customer_id"]
            )
        ).sum()

        missing_product_count = (
            ~order_detail_df["product_id"].isin(
                product_df["product_id"]
            )
        ).sum()

        logger.info(
            f"找不到订单的明细数：{orphan_detail_count}"
        )

        logger.info(
            f"没有商品明细的订单数：{missing_detail_order_count}"
        )

        logger.info(
            f"找不到客户信息的订单数：{missing_customer_count}"
        )

        logger.info(
            f"找不到商品信息的明细数：{missing_product_count}"
        )
        detail_count = (
            order_detail_df
            .groupby("order_id")
            .size()
        )

        multi_detail_order_count = (
                detail_count > 1
        ).sum()

        max_detail_count = detail_count.max()

        logger.info(
            f"包含多条商品明细的订单数："
            f"{multi_detail_order_count}"
        )

        logger.info(
            f"单个订单最大明细数："
            f"{max_detail_count}"
        )
        # 第一次关联
        # 订单 + 明细

        df = order_df.merge(
            order_detail_df,
            on="order_id",
            how="left",
            validate="one_to_many"
        )
        logger.info(
            "订单明细关联完成"
        )

        # ======================
        # 4.关联客户
        # ======================

        df = df.merge(

            customer_df,

            on="customer_id",

            how="left",

            validate="many_to_one",

            suffixes=("", "_customer")

        )
        logger.info(
            "客户关联完成"
        )

        # ======================
        # 关联商品
        # ======================

        df = df.merge(
            product_df,
            on="product_id",
            how="left",
            validate="many_to_one",
            suffixes=("", "_product")
        )
        logger.info(
            "商品关联完成"
        )

        # =====================
        # 字段控制
        # =====================
        df = df[DWD_ORDER_COLUMNS]

        # 保存目录

        self.save(
            df,
            output_file
        )

        logger.info(
            f"DWD生成完成:{output_file}"
        )


        return df


