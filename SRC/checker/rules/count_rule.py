from checker.rules.base_rule import BaseRule
from checker.quality_result import QualityResult

class CountRule(BaseRule):
    def __init__(self,min_count):
        self.min_count = min_count


    def check(self,df):
        count = len(df)
        if count < self.min_count:
            return QualityResult(
                rule_name='CountRule',
                status='FAILED',
                error_count=1,
                message=f'数据量异常:{count}<{self.min_count}'
            )
        return QualityResult(

            rule_name="CountRule",

            status="SUCCESS",

            error_count=0,

            message=
            f"数据量正常:{count}"

        )
