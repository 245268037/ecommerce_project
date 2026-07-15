import pandas as pd
import os
from utils.reader import read_csv_with_schema
from config.schema import ORDER_SCHEMA


class DWDBuilder:

    def build_order_detail(
            self,
            order_file,
            order_detail_file,
            customer_file,
            product_file,
            output_file
    ):
        # 读取ODS数据

        order_df = read_csv_with_schema(
            order_file,
            ORDER_SCHEMA
        )

        order_detail_df = read_csv_with_schema(
            order_detail_file,
            ORDER_SCHEMA
        )

        customer_df = read_csv_with_schema(
            customer_file,
            ORDER_SCHEMA
        )

        product_df = read_csv_with_schema(
            product_file,
            ORDER_SCHEMA
        )

        order_detail_df = order_detail_df.drop(columns=['etl_time','source_system','etl_batch'],errors='ignore')
        customer_df = customer_df.drop(columns=['etl_time','source_system','etl_batch'],errors='ignore')
        product_df = product_df.drop(columns=['etl_time','source_system','etl_batch'],errors='ignore')

        # 第一次关联
        # 订单 + 明细

        df = order_df.merge(
            order_detail_df,
            on="order_id",
            how="left",
            validate="one_to_many"
        )


        # 第二次关联
        # 客户信息

        df = df.merge(
            customer_df,
            on="customer_id",
            how="left",
            validate="many_to_one"
        )


        # 第三次关联
        # 商品信息

        df = df.merge(
            product_df,
            on="product_id",
            how="left",
            validate="many_to_one"
        )


        # 保存目录

        os.makedirs(
            os.path.dirname(output_file),
            exist_ok=True
        )


        df.to_csv(
            output_file,
            index=False,
            encoding="utf-8-sig"
        )


        print(
            f"DWD生成完成:{output_file}"
        )


        return df


