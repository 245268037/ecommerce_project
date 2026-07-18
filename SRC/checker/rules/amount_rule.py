from checker.rules.base_rule import BaseRule
from checker.quality_result import QualityResult


class AmountRule(BaseRule):
    def check(self,df):
        # 找异常数据
        error_df = df[
            df["payable_amount"] < 0
            ]
        return QualityResult(
            rule_name="AmountRule",
            status=
            "FAILED"
            if len(error_df) > 0
            else
            "SUCCESS",
            error_count=len(error_df),
            data=error_df,
            message=
            "存在负金额"
            if len(error_df) > 0
            else
            "金额校验通过"
        )