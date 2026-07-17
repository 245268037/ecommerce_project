from .base_rule import BaseRule

class AmountRule(BaseRule):
    def check(self,df):
        return df[df['payable_amount']<0]