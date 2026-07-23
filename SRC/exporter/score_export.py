import pandas as pd
import os


class ScoreExport:
    def export(
            self,
            score_result,
            output_file
    ):
        # 创建目录
        os.makedirs(
            os.path.dirname(output_file),
            exist_ok=True
        )
        # ======================
        # 1.生成总览数据
        # ======================

        summary = pd.DataFrame(
            [
                {
                "total_score":
                    score_result["total_score"],
                "level":
                    score_result["level"]
                }
            ]
        )


        # ======================
        # 2.生成规则明细
        # ======================
        detail = pd.DataFrame(
            score_result["detail"]
        )

        # ======================
        # 3.写Excel
        # ======================

        with pd.ExcelWriter(
            output_file,
            engine="openpyxl"
        ) as writer:
            summary.to_excel(
                writer,
                sheet_name="质量评分总览",
                index=False
            )
            detail.to_excel(
                writer,
                sheet_name="规则评分明细",
                index=False
            )
