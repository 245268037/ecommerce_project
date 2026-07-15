import pandas as pd

class standard_cleaner():
    def check_type(self,dif):
        # 改变数据类型
        def check_type(self, dif):
            money_cols = ['order_amount', 'coupon_amount', 'freight_amount', 'payable_amount']
            date_cols = ['order_time', 'updated_at']
            code_cols = ['receiver_province_code', 'province_code', 'shop_id', 'customer_id', 'product_id']

            for col in money_cols:
                if col in dif.columns:
                    dif[col] = pd.to_numeric(dif[col], errors='coerce')
            for col in date_cols:
                if col in dif.columns:
                    dif[col] = pd.to_datetime(dif[col], errors='coerce')
            for col in code_cols:
                if col in dif.columns:
                    dif[col] = (dif[col].astype(str).str.strip())

            
            return dif