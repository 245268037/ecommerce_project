from checker.quality_result import QualityResult
from checker.rules.base_rule import BaseRule

class NullRule(BaseRule):

    def check(self,df):
        error_df = df[
            df.isnull().any(axis=1)
        ]
        return QualityResult(
            rule_name = 'NullRule',
            status = 'FAILED'
            if  len(error_df) > 0
            else 'SUCCESS',
            error_count=len(error_df),
            data = error_df,
            message=
            "存在空值"
            if len(error_df) > 0
            else
            "无空值"
        )