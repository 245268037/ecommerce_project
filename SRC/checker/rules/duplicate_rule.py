from checker.rules.base_rule import BaseRule
from checker.quality_result import QualityResult

class DuplicateRule(BaseRule):
    def __init__(self,columns):
        self.columns = columns

    def check(self, df):
        error_df =  df[
            df.duplicated(
                subset=self.columns,
                keep=False
            )
        ]
        return QualityResult(
            rule_name="DuplicateRule",

            status=
            "FAILED"
            if len(error_df) > 0
            else
            "SUCCESS",

            error_count=len(error_df),

            data=error_df,

            message=
            "存在重复数据"
            if len(error_df) > 0
            else
            "无重复数据"
        )