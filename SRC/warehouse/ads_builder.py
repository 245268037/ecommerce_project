import pandas as pd
import os
from warehouse.base_builder import BaseBuilder
from config.dwd_schema import DWD_ORDER_DETAIL_SCHEMA
from utils.logger import logger



class ADSBuilder(BaseBuilder):

    def build_sales_summary(self,dwd_file,output):
        logger.info('开始生成销售指标')
        df = self.read(dwd_file, DWD_ORDER_DETAIL_SCHEMA)
        #时间字符串转换
        df['order_time'] = pd.to_datetime(df['order_time'],errors='coerce')
        #按日期统计
        df['order_date'] = (df['order_time'].dt.strftime('%Y-%m-%d'))


        sales = df.groupby('order_date').agg(
            order_count = ('order_id','count'),
            user_count = ('customer_id','nunique'),
            sales_amount = ('payable_amount','sum')
        ).reset_index()

        #平均订单金额
        sales['avg_order_amount'] = (sales['sales_amount'] / sales['order_count']).round(2)

        self.save(sales,output)
        logger.info(
            f"销售指标完成:{output}"
        )
        return sales



    def build_user_summary(
            self,
            dwd_file,
            output
    ):
        df = self.read(dwd_file,DWD_ORDER_DETAIL_SCHEMA)
        logger.info('开始生成用户指标')
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
        self.save(user,output)
        logger.info(f'用户指标保存完成:{output}')
        return user


    def build_product_summary(
            self,
            dwd_file,
            output
    ):
        df = self.read(dwd_file,DWD_ORDER_DETAIL_SCHEMA)
        logger.info('开始生成商品指标')
        # 商品运营指标
        product = (df.groupby(['product_id', 'product_name', 'category_name'])).agg(
            sales_count=('quantity', 'sum'),
            sales_amount=('actual_amount', 'sum'),
        ).reset_index()

        product['rank'] = product['sales_count'].rank(ascending=False)
        self.save(product,output)
        logger.info(f'商品指标保存完成:{output}')






