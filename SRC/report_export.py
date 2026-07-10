import pandas as pd

class ReportExport:

    def export(self,null_report,duplicate,amount_erro,status_erro):
        with pd.ExcelFile('../data/report/quality_report.xlsx') as writer:
            null_report.to_excel(writer,sheet_name='空值统计',index=False)
            duplicate.to_excel(writer,sheet_name= '重复数据',index=False)
            amount_erro.to_excel(writer,sheet_name='异常金额',index=False)
            status_erro.to_excel(writer,sheet_name='异常状态',index=False)

