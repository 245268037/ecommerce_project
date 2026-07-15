import pandas as pd
import os

class ReportExport:

    def export(
            self,
            null_report,
            duplicate,
            business_error,
            amount_error
    ):
        output_file = '../data/report/quality_report.xlsx'
        #创建目录
        os.makedirs(
            os.path.dirname(output_file),
            exist_ok=True
        )

        with pd.ExcelWriter(output_file,engine='openpyxl') as writer:
            null_report.to_excel(writer,sheet_name='空值统计',index=False)
            duplicate.to_excel(writer,sheet_name= '重复数据',index=False)
            business_error.to_excel(writer,sheet_name='业务异常',index=False)
            amount_error.to_excel(writer,sheet_name='金额异常',index=False)

        print( f"质量报告生成完成:{output_file}")

