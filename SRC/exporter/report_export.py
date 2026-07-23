import pandas as pd
import os

class ReportExport:

    def export(
            self,
            results,
            output_file
    ):
        #创建目录
        os.makedirs(
            os.path.dirname(output_file),
            exist_ok=True
        )

        summary = []
        with pd.ExcelWriter(
                output_file,
                engine="openpyxl"
        ) as writer:
            for rule_name, result in results.items():
                summary.append(
                    {'rule_name': result.rule_name,
                     'status':result.status,
                     'error_count':result.error_count,
                     'message':result.message
                     }
                )
                if result.data is not None:
                    result.data.to_excel(
                        writer,
                        sheet_name=rule_name,
                        index=False
                    )
                    # =====================
                    # 3.生成质量汇总Sheet
                    # =====================

                    summary_df = pd.DataFrame(
                        summary
                    )

                    summary_df.to_excel(
                        writer,
                        sheet_name="质量汇总",
                        index=False
                    )
