from .base_rule import BaseRule

class BusinessRule(BaseRule):
    def check(self,df):
        return df.query(
            'payable_amount != order_amount - coupon_amount + freight_amount'
        )