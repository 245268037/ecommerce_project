import pandas as pd
import os
from utils.reader import read_csv_with_schema
from config.schema import ORDER_SCHEMA



class ADSBuilder():

    def build_sales_summary(self,dwd_file,output):
        df = read_csv_with_schema(dwd_file,ORDER_SCHEMA)

        #时间字符串转换
        df['order_time'] = pd.to_datetime(df['order_time'],errors='coerce')
        #按日期统计
        df['order_date'] = (df['order_time'].dt.date)


        sales = df.groupby('order_date').agg(
            order_count = ('order_id','count'),
            user_count = ('customer_id','nunique'),
            sales_amount = ('payable_amount','sum')
        ).reset_index()

        #平均订单金额
        sales['avg_order_amount'] = (sales['sales_amount'] / sales['order_count']).round(2)

        os.makedirs(os.path.dirname(output), exist_ok=True)
        sales.to_csv(
            output,
            index = False
        )




    def build_user_summary(
            self,
            dwd_file,
            output
    ):
        df = read_csv_with_schema(dwd_file,ORDER_SCHEMA)
        # 用户运营指标
        user = df.groupby(['customer_id', 'customer_name']).agg(
            order_count=('order_id', 'count'),
            total_amount=('payable_amount', 'sum')
        ).reset_index()

        user['avg_amount'] = (user['total_amount'] / user['order_count']).round(2)

        def level(x):
            if x >= 10000:
                return 'VIP'
            elif x > 5000:
                return '重点用户'
            else:
                return '普通用户'

        user['user_level'] = (user['total_amount'].apply(level))
        os.makedirs(os.path.dirname(output), exist_ok=True)
        user.to_csv(
            output,
            index=False
        )




    def build_product_summary(
            self,
            dwd_file,
            output
    ):
        df = read_csv_with_schema(dwd_file,ORDER_SCHEMA)
        # 商品运营指标
        product = (df.groupby(['product_id', 'product_name', 'category_name'])).agg(
            sales_count=('quantity', 'sum'),
            sales_amount=('actual_amount', 'sum'),
        ).reset_index()

        product['rank'] = product['sales_count'].rank(ascending=False)
        os.makedirs(os.path.dirname(output), exist_ok=True)

        product.to_csv(
            output,
            index = False
        )






