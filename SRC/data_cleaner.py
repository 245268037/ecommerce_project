import pandas as pd

class DataCleaner:

    #删除重复数据
    def check_duplicate(self,dif):
        #只拿重复行
        duplicate_count  = dif[dif.duplicated()]
        #删除重复行数据，保留第一次出现，赋值到dif，重置索引
        dif = dif.drop_duplicates().reset_index(drop=True)

        return dif,duplicate_count

    #改变数据类型
    def check_type(self,dif):
        money_cols = ['order_amount', 'coupon_amount', 'freight_amount', 'payable_amount']
        date_cols = ['order_time', 'updated_at']

        for col in money_cols:
            if col  in dif.columns:
                dif[col] = pd.to_numeric(dif[col],errors='coerce')
        for col in date_cols:
            if col in dif.columns:
                dif[col] = pd.to_datetime(dif[col],errors='coerce')
        return dif

    #给空值赋值
    def check_fillna(self,dif):
        money_cols = ['order_amount', 'coupon_amount', 'freight_amount', 'payable_amount']

        for col in money_cols:
            if col in dif.columns:
                dif[col] = dif[col].fillna(0)
        return dif

#去除空字符
    def trim_text(self,dif):
        text_cols = [
            "province",
        "city",
        "receiver"]

        for col in text_cols:
            if col in dif.columns:
                #去除空字符串
                dif[col] = dif[col].str.strip()
        return dif

#支付状态校验
    def check_status(self,dif):
        status = [

            "待支付",

            "已支付",

            "已发货",

            "已完成",

            "已取消",

            "已退款"

        ]
        error = dif[~dif['order_status'].isin(status)]
        return error

#订单金额
    def check_business(self, df):

        error = df.query(
           'payable_amount !=  order_amount -coupon_amount+   freight_amount'
        )

        return error