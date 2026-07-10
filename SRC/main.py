from data_loader import DataLoader
from data_cleaner import DataCleaner
from quality_checker import QualityChecker
from report_export import ReportExport
from ods_builder import  ODSBuilder


loader = DataLoader()
cleaner = DataCleaner()
cheker = QualityChecker()
reporter = ReportExport()
ods = ODSBuilder()


df = loader.load_data('../data/raw/order.csv')

# df, duplicate = cleaner.check_duplicate(df)
# df = cleaner.check_fillna(df)
# df = cleaner.check_type(df)
# null_report = cleaner.trim_text(df)
# amount_erro = cleaner.check_business(df)
# status_err = cleaner.check_status(df)
# reporter.export(
#     null_report,
#     duplicate,
#     amount_erro,
#     status_err
# )
#print('数据质检完成')

ods.build_ods('../data/raw/order.csv','../warehouse/ods/ods_customer,csv')
ods.build_ods(

    "../data/raw/order.csv",

    "../warehouse/ods/ods_order.csv"

)
ods.build_ods(

    "../data/raw/product.csv",

    "../warehouse/ods/ods_product.csv"

)

print('ODS完成')


