import pandas as pd
from utils.reader import read_csv_with_schema
from config.schema import ORDER_SCHEMA
import os

class DataCleaner:

    #删除重复数据
    def check_duplicate(self,df):
        #只拿重复行
        duplicate_count  = df[df.duplicated()]
        #删除重复行数据，保留第一次出现，df，重置索引
        df = df.drop_duplicates().reset_index(drop=True)

        return df,duplicate_count

    #改变数据类型
    def check_type(self,df):
        money_cols = ['order_amount', 'coupon_amount', 'freight_amount', 'payable_amount']
        date_cols = ['order_time', 'updated_at']
        code_cols = ['receiver_province_code','province_code','shop_id','customer_id','product_id']

        for col in money_cols:
            if col  in df.columns:
                df[col] = pd.to_numeric(df[col],errors='coerce')
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col],errors='coerce')
        for col in code_cols:
            if col in df.columns:
                df[col] = (df[col].astype(str).str.strip())
        return df

    #给空值赋值
    def check_fillna(self,df):
        money_cols = ['order_amount', 'coupon_amount', 'freight_amount', 'payable_amount']

        for col in money_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df

#去除空字符
    def trim_text(self,df):
        text_cols = [
            "province",
        "city",
        "receiver"]

        for col in text_cols:
            if col in df.columns:
                #去除空字符串
                df[col] = df[col].str.strip()
        return df

#支付状态校验
    def check_status(self,df):
        status = [

            "待支付",

            "已支付",

            "已发货",

            "已完成",

            "已取消",

            "已退款"

        ]
        error = df[~df['order_status'].isin(status)]
        return error

#订单金额
    def check_business(self, df):

        error = df.query(
           'payable_amount !=  order_amount -coupon_amount+   freight_amount'
        )

        return error

    def clean(self, input_file,output_file):

        df = read_csv_with_schema(
            input_file,
            ORDER_SCHEMA
        )

        # 删除重复
        df,duplicate_df  = self.check_duplicate(df)

        # 空值处理
        df = self.check_fillna(df)

        # 类型转换
        df = self.check_type(df)


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
            f"清洗完成:{output_file}"
        )
