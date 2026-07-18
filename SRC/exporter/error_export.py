import pandas as pd
import os



class ErrorExport:
    def export(
            self,
            results,
            output
    ):
        error_list=[]

        for rule_name,result in results.items():
            if result.data is not None:
                df=result.data.copy()
                df["rule_name"]=rule_name
                df["severity"]=result.severity
                error_list.append(df)



        if error_list:
            error_df=pd.concat(
                error_list,
                ignore_index=True
            )
        else:
            error_df=pd.DataFrame()

        os.makedirs(
            os.path.dirname(output),
            exist_ok=True
        )

        error_df.to_excel(
            output,
            index=False

        )