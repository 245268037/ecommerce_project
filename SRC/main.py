from warehouse.ods_builder import ODSBuilder
from warehouse.dwd_builder import DWDBuilder
from warehouse.dws_builder import DWSbuilder
from cleaner.data_cleaner import DataCleaner
from checker.quality_checker import QualityChecker
from exporter.report_export import ReportExport
from warehouse.ads_builder import ADSBuilder
import pandas as pd
import os


def main():
        base_dir = os.path.dirname(os.path.dirname(__file__))

        cleaner = DataCleaner()
        quality_checker = QualityChecker()
        reporter = ReportExport()
        ods = ODSBuilder()
        dwd = DWDBuilder()
        dws = DWSbuilder()
        ads = ADSBuilder()

        #清洗路径
        cleaner_order = os.path.join(
            base_dir,
            'data',
            'clean',
            'cleaner_order.csv'
        )
        cleaner_customer = os.path.join(
            base_dir,
            'data',
            'clean',
            'cleaner_customer.csv'
        )
        cleaner_order_detail = os.path.join(
            base_dir,
            'data',
            'clean',
            'cleaner_order_detail.csv'
        )
        cleaner_product = os.path.join(
            base_dir,
            'data',
            'clean',
            'cleaner_product.csv'
        )



        # 原始数据路径
        raw_order = os.path.join(base_dir, "data", "raw", "order.csv")
        raw_order_detail = os.path.join(base_dir, "data", "raw", "order_detail.csv")
        raw_customer = os.path.join(base_dir, "data", "raw", "customer.csv")
        raw_product = os.path.join(base_dir, "data", "raw", "product.csv")

        print('开始清洗数据')
        #订单
        cleaner.clean(raw_order, cleaner_order)


        #客户
        cleaner.clean(raw_customer,cleaner_customer)
        #商品
        cleaner.clean(raw_product,cleaner_product)
        #订单明细
        cleaner.clean(raw_order_detail,cleaner_order_detail)
        print('数据清洗完成')

        #数据检测
        print("=" * 60)
        print("开始数据质量检查")
        print("=" * 60)

        order_df = pd.read_csv(
            cleaner_order,
            dtype={'receiver_province_code':str}
        )
        null_report = quality_checker.check_null(order_df)
        duplicate_report = quality_checker.check_duplicate(order_df,['order_id'])
        business_error = quality_checker.check_business(order_df)
        amount_error = quality_checker.check_amount(order_df)



        print("空值检查完成")
        print("重复检查完成")
        print("业务检查完成")
        print("金额检查完成")

        reporter.export(
            null_report,
            duplicate_report,
            business_error,
            amount_error
        )


        # ODS路径
        ods_order = os.path.join(base_dir, "warehouse", "ods", "ods_order.csv")
        ods_order_detail = os.path.join(base_dir, "warehouse", "ods", "ods_order_detail.csv")
        ods_customer = os.path.join(base_dir, "warehouse", "ods", "ods_customer.csv")
        ods_product = os.path.join(base_dir, "warehouse", "ods", "ods_product.csv")

        # DWD路径
        dwd_order = os.path.join(
            base_dir,
            "warehouse",
            "dwd",
            "dwd_order_detail.csv"
        )
        print("=" * 60)
        print("开始构建ODS层")
        print("=" * 60)

        ods.build_ods(cleaner_order, ods_order)
        ods.build_ods(cleaner_order_detail, ods_order_detail)
        ods.build_ods(cleaner_customer, ods_customer)
        ods.build_ods(cleaner_product, ods_product)

        print("ODS层完成")



        print("=" * 60)
        print("开始构建DWD层")
        print("=" * 60)

        dwd.build_order_detail(
            ods_order,
            ods_order_detail,
            ods_customer,
            ods_product,
            dwd_order
        )
        print("DWD层完成")
        print("=" * 60)
        print("数据仓库构建完成")
        print("=" * 60)

        print("=" * 60)
        print("开始构建DWS层")
        print("=" * 60)

        dws_user_sales = os.path.join(
            base_dir,
            "warehouse",
            "dws",
            "dws_user_sales.csv"
        )
        dws.build_user_sales(
            dwd_order,
            dws_user_sales
        )
        print('dws完成')

        print("=" * 60)
        print("开始构建ADS指标")
        print("=" * 60)

        ads_sales  = os.path.join(
            base_dir,
            'warehouse',
            'ads',
            'ads_sales_summary.csv'
        )
        ads.build_sales_summary(dwd_order,
                               ads_sales )

        ads_user = os.path.join(
            base_dir,
            'warehouse',
            'ads',
            'ads_user_summary.csv'
        )
        ads.build_user_summary(dwd_order,ads_user)

        ads_product = os.path.join(
            base_dir,
            'warehouse',
            'ads',
            'ads_product_summary.csv'
        )
        ads.build_product_summary(dwd_order, ads_product)

        print("ADS销售指标完成")

if __name__ == "__main__":
    main()


