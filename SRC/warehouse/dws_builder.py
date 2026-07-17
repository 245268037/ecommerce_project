import pandas as pd
from warehouse.base_builder import BaseBuilder
from utils.reader import read_csv_with_schema
from config.dwd_schema import (
    DWD_ORDER_DETAIL_SCHEMA
)
from utils.logger import logger


class DWSbuilder(BaseBuilder):

    def build_user_sales(
            self,
            dwd_file,
            output_file
    ):
        logger.info(
            "开始构建用户销售主题"
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
        # 用户销售主题
        # =====================

        user_sales = df.groupby(
            [
                'customer_id',
                'customer_name'
            ]
        ).agg(
            order_count=(
                'order_id',
                'count'
            ),
            total_amount=(
                'payable_amount',
                'sum'
            ),
            first_order_time=(
                'order_time',
                'min'
            ),
            last_order_time=(
                'order_time',
                'max'
            )
        ).reset_index()

        user_sales['avg_amount'] = (
            user_sales['total_amount']
            /
            user_sales['order_count']
        ).round(2)
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
        df = self.read(
            dwd_file,
            DWD_ORDER_DETAIL_SCHEMA
        )
        product_sales = df.groupby(
            [
                "product_id",
                "product_name",
                "category_name"
            ]
        ).agg(
            sales_count=(
                "quantity",
                "sum"
            ),
            sales_amount=(
                "actual_amount",
                "sum"
            )
        ).reset_index()
        self.save(
            product_sales,
            output_file
        )
        logger.info(
            "商品销售主题完成"
        )
        return product_sales
