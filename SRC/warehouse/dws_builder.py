import pandas as pd
import os
from utils.reader import read_csv_with_schema
from config.schema import ORDER_SCHEMA


class DWSbuilder:


    def build_user_sales(
            self,
            dwd_file,
            output_file
    ):


        df = read_csv_with_schema(
            dwd_file,
            ORDER_SCHEMA
        )


        print(df.columns)


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



        os.makedirs(
            os.path.dirname(output_file),
            exist_ok=True
        )


        user_sales.to_csv(
            output_file,
            index=False,
            encoding='utf-8-sig'
        )


        # =====================
        # 商品销售主题
        # =====================


        product_sales=df.groupby(
            [
                'product_id',
                'product_name',
                'category_name'
            ]
        ).agg(

            sales_count=(
                'quantity',
                'sum'
            ),

            sales_amount=(
                'actual_amount',
                'sum'
            )

        ).reset_index()



        product_output=os.path.join(
            os.path.dirname(output_file),
            "dws_product_sales.csv"
        )


        product_sales.to_csv(
            product_output,
            index=False,
            encoding='utf-8-sig'
        )



        print("DWS层构建完成")