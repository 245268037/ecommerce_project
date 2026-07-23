import pandas as pd
import os

from utils.reader import read_csv_with_schema


class DataCleaner:

    #删除重复数据
    def check_duplicate(self, df):
        duplicate_df = df[df.duplicated()]

        df = (
            df
            .drop_duplicates()
            .reset_index(drop=True)
        )

        return df, duplicate_df

    #改变数据类型
    def check_type(self,df):
        money_cols = [
            'order_amount',

            'coupon_amount',

            'freight_amount',

            'payable_amount',

            'unit_price',

            'actual_amount'

        ]
        date_cols = [

            'order_time',

            'updated_at',

            'created_at',

            'register_time'

        ]
        code_cols = [

            'receiver_province_code',

            'province_code',

            'shop_id',

            'customer_id',

            'product_id'
        ]
        # 金额
        for col in money_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col],errors="coerce")
        # 日期
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col],errors="coerce")
        # 编码
        for col in code_cols:
            if col in df.columns:
                df[col] = (df[col].astype("string").str.strip())

        return df

    #给空值赋值
    def check_fillna(self,df):
        money_cols = ['order_amount', 'coupon_amount', 'freight_amount', 'payable_amount',"actual_amount"]
        for col in money_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df

#去除空字符
    def trim_text(self,df):
        for col in df.columns:
            if df[col].dtype=="object":
                df[col]=(df[col].astype(str).str.strip())
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

    def clean(
            self,
            input_file,
            output_file,
            schema
    ):

        # 1.读取
        df = read_csv_with_schema(
            input_file,
            schema
        )


        # 2.去重

        df, _ = self.check_duplicate(df)

        # 3.空值

        df = self.check_fillna(df)

        # 4.类型

        df = self.check_type(df)

        # 5.文本

        df = self.trim_text(df)

        # 6.保存
        os.makedirs(
            os.path.dirname(output_file),
            exist_ok=True
        )

        df.to_csv(
            output_file,
            index=False,
            encoding="utf-8-sig"
        )

        return df
