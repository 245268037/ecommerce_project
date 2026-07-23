import pandas as pd
import os
from datetime import datetime



class HistoryWriter:


    def write(
            self,
            score_result,
            output_file
    ):


        # =========================
        # 1.当前质量记录
        # =========================


        record = {
            "date":
                datetime.now().strftime(
                    "%Y-%m-%d"
                ),
            "score":
                score_result["total_score"],
            "level":
                score_result["level"]
        }


        # 转DataFrame
        new_df = pd.DataFrame(
            [record]
        )
        # =========================
        # 2.判断历史文件是否存在
        # =========================


        if os.path.exists(output_file):
            old_df = pd.read_csv(
                output_file
            )
            df = pd.concat(
                [
                    old_df,
                    new_df
                ],
                ignore_index=True
            )
        else:
            df = new_df



        # =========================
        # 3.保存
        # =========================


        os.makedirs(
            os.path.dirname(output_file),
            exist_ok=True
        )


        df.to_csv(
            output_file,
            index=False,
            encoding="utf-8-sig"
        )
