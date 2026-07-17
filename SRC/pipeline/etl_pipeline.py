import os
import pandas as pd

from config.clean_schema import CLEAN_ORDER_SCHEMA
from cleaner.data_cleaner import DataCleaner
from exporter.report_export import ReportExport


from warehouse.ods_builder import ODSBuilder
from warehouse.dwd_builder import DWDBuilder
from warehouse.dws_builder import DWSbuilder
from warehouse.ads_builder import ADSBuilder


from checker.rule_engine import RuleEngine
from checker.rules.null_rule import NullRule
from checker.rules.duplicate_rule import DuplicateRule
from checker.rules.amount_rule import AmountRule
from checker.rules.business_rule import BusinessRule
from config.raw_schema import (
    ORDER_RAW_SCHEMA,
    ORDER_DETAIL_RAW_SCHEMA,
    CUSTOMER_RAW_SCHEMA,
    PRODUCT_RAW_SCHEMA
)
from config import path
from config.raw_schema import ORDER_RAW_SCHEMA
from utils.file_checker import FileChecker
from utils.logger import logger
from utils.reader import read_csv_with_schema

class ETLPipeline:
    def __init__(self):
        self.cleaner = DataCleaner()
        self.reporter = ReportExport()
        self.ods = ODSBuilder()
        self.dwd = DWDBuilder()
        self.dws = DWSbuilder()
        self.ads = ADSBuilder()

    def check_file(self):

        logger.info(
            "开始检查输入文件"
        )

        FileChecker.check_exists(
            path.RAW_ORDER
        )

        FileChecker.check_exists(
            path.RAW_ORDER_DETAIL
        )

        FileChecker.check_exists(
            path.RAW_CUSTOMER
        )

        FileChecker.check_exists(
            path.RAW_PRODUCT
        )

        logger.info(
            "输入文件检查完成"
        )

    def run(self):
        try:
            logger.info(
                "ETL任务开始"
            )

            self.check_file()

            self.clean()

            self.quality_check()

            self.build_ods()

            self.build_dwd()

            self.build_dws()

            self.build_ads()

            logger.info("ETL任务完成")

        except  Exception as e:
            logger.exception(
                f'ETL任务执行失败{e}'
            )
            raise

        # =========================
        # 数据质量
        # =========================

    def clean(self):
        logger.info(
            "开始数据清洗"
        )

        self.cleaner.clean(
            path.RAW_ORDER,
            path.CLEAN_ORDER,
            ORDER_RAW_SCHEMA
        )

        self.cleaner.clean(
            path.RAW_ORDER_DETAIL,
            path.CLEAN_ORDER_DETAIL,
            ORDER_DETAIL_RAW_SCHEMA
        )

        self.cleaner.clean(
            path.RAW_CUSTOMER,
            path.CLEAN_CUSTOMER,
            CUSTOMER_RAW_SCHEMA
        )
        self.cleaner.clean(
            path.RAW_PRODUCT,
            path.CLEAN_PRODUCT,
            PRODUCT_RAW_SCHEMA
        )
        logger.info(
            "数据清洗完成"
        )

    def quality_check(self):
        df = read_csv_with_schema(
            path.CLEAN_ORDER,
            CLEAN_ORDER_SCHEMA
        )
        engine = RuleEngine()
        engine.register(
            NullRule()
        )
        engine.register(
            DuplicateRule(
                ["order_id"]
            )
        )
        engine.register(
            AmountRule()
        )

        engine.register(
            BusinessRule()
        )
        result = engine.run(df)

        self.reporter.export(
            result["NullRule"],
            result["DuplicateRule"],
            result["BusinessRule"],
            result["AmountRule"]
        )

        # =========================
        # ODS
        # =========================

    def build_ods(self):
        logger.info(
            "ODS开始"
        )

        self.ods.build_ods(
            path.CLEAN_ORDER,
            path.ODS_ORDER,
            ORDER_RAW_SCHEMA
        )

        self.ods.build_ods(
            path.CLEAN_ORDER_DETAIL,
            path.ODS_ORDER_DETAIL,
            ORDER_DETAIL_RAW_SCHEMA
        )

        self.ods.build_ods(
            path.CLEAN_CUSTOMER,
            path.ODS_CUSTOMER,
            CUSTOMER_RAW_SCHEMA
        )

        self.ods.build_ods(
            path.CLEAN_PRODUCT,
            path.ODS_PRODUCT,
            PRODUCT_RAW_SCHEMA
        )

        # =========================
        # DWD
        # =========================

    def build_dwd(self):
        self.dwd.build_order_detail(
            path.ODS_ORDER,
            path.ODS_ORDER_DETAIL,
            path.ODS_CUSTOMER,
            path.ODS_PRODUCT,
            path.DWD_ORDER_DETAIL
        )

        # =========================
        # DWS
        # =========================

    def build_dws(self):
        self.dws.build_user_sales(
            path.DWD_ORDER_DETAIL,
            path.DWS_USER_SALES
        )
        self.dws.build_product_sales(
            path.DWD_ORDER_DETAIL,
            path.DWS_PRODUCT_SALES

        )

        # =========================
        # ADS
        # =========================

    def build_ads(self):
        self.ads.build_sales_summary(
            path.DWD_ORDER_DETAIL,
            path.ADS_SALES_SUMMARY

        )

        self.ads.build_user_summary(
            path.DWD_ORDER_DETAIL,
            path.ADS_USER_SUMMARY

        )

        self.ads.build_product_summary(
            path.DWD_ORDER_DETAIL,
            path.ADS_PRODUCT_SUMMARY

        )