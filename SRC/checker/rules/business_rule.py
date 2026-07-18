from checker.rules.base_rule import BaseRule
from checker.quality_result import QualityResult

class BusinessRule(BaseRule):
    def check(self,df):
        error_df =  df.query( 'payable_amount != order_amount - coupon_amount + freight_amount')
        return QualityResult(
            rule_name='BusinessRule',
            status='FIANDE'
            if len(error_df) > 0
            else 'SUCCESS',
            error_count=len(error_df),
            message='订单金额异常'
            if len(error_df) > 0
            else '业务校验通过'
        )