from checker.rules.base_rule import BaseRule
from checker.quality_result import QualityResult



class TypeRule(BaseRule):


    def __init__(self, schema):

        self.schema = schema



    def check(self, df):


        errors = []


        for col, dtype in self.schema.items():


            if col not in df.columns:

                errors.append(
                    f"{col}字段不存在"
                )

                continue


            actual_type = str(
                df[col].dtype
            )


            if dtype == "string":


                if not (
                    actual_type.startswith("object")
                    or
                    actual_type.startswith("string")
                ):

                    errors.append(
                        f"{col}类型错误，实际:{actual_type}"
                    )


            elif dtype == "number":


                if not (
                    actual_type.startswith("int")
                    or
                    actual_type.startswith("float")
                ):

                    errors.append(
                        f"{col}类型错误，实际:{actual_type}"
                    )



        if errors:


            return QualityResult(

                rule_name="TypeRule",

                status="FAILED",

                error_count=len(errors),

                data=None,

                message=";".join(errors)

            )


        return QualityResult(

            rule_name="TypeRule",

            status="SUCCESS",

            error_count=0,

            data=None,

            message="字段类型正常"

        )