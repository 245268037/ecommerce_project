import pandas as pd
from warehouse.base_builder import BaseBuilder
from config.dwd_schema import DWD_ORDER_DETAIL_SCHEMA
from utils.logger import logger
from config.settings import VALID_SALES_STATUSES
import numpy as np



class ADSBuilder(BaseBuilder):

    def build_sales_summary(
            self,
            dwd_file,
            output
    ):
        logger.info(
            "开始生成ADS销售指标"
        )

        df = self.read(
            dwd_file,
            DWD_ORDER_DETAIL_SCHEMA
        )

        # =====================
        # 过滤有效销售订单
        # =====================

        all_order_count = (
            df["order_id"].nunique()
        )

        df = df[
            df["order_status"].isin(
                VALID_SALES_STATUSES
            )
        ].copy()

        valid_order_count = (
            df["order_id"].nunique()
        )

        logger.info(
            f"ADS销售口径过滤："
            f"全部订单数={all_order_count}，"
            f"有效订单数={valid_order_count}，"
            f"排除订单数="
            f"{all_order_count - valid_order_count}"
        )


        # =====================
        # 1.恢复订单粒度
        # =====================

        order_df = (
            df.drop_duplicates(
                subset=["order_id"]
            )
            .copy()
        )

        logger.info(
            f"DWD明细行数：{len(df)}，"
            f"订单粒度行数：{len(order_df)}"
        )

        # =====================
        # 2.转换订单时间
        # =====================

        order_df["order_time"] = pd.to_datetime(
            order_df["order_time"],
            errors="coerce"
        )

        invalid_time_count = (
            order_df["order_time"]
            .isna()
            .sum()
        )

        logger.info(
            f"订单时间无效数量："
            f"{invalid_time_count}"
        )

        # 日销售指标无法统计没有日期的订单
        valid_order_df = (
            order_df[
                order_df["order_time"].notna()
            ]
            .copy()
        )

        valid_order_df["order_date"] = (
            valid_order_df["order_time"]
            .dt.strftime("%Y-%m-%d")
        )

        # =====================
        # 3.按日期统计
        # =====================

        sales = (
            valid_order_df
            .groupby(
                "order_date",
                dropna=False
            )
            .agg(
                order_count=(
                    "order_id",
                    "nunique"
                ),
                user_count=(
                    "customer_id",
                    "nunique"
                ),
                sales_amount=(
                    "payable_amount",
                    "sum"
                )
            )
            .reset_index()
        )

        # =====================
        # 4.计算平均订单金额
        # =====================

        sales["avg_order_amount"] = (
                sales["sales_amount"]
                /
                sales["order_count"]
        ).round(2)

        # =====================
        # 5.核对订单数量
        # =====================

        source_order_count = (
            valid_order_df["order_id"]
            .nunique()
        )

        ads_order_count = (
            sales["order_count"]
            .sum()
        )

        logger.info(
            f"ADS销售指标订单核对："
            f"源订单数={source_order_count}，"
            f"ADS订单数={ads_order_count}"
        )

        if source_order_count != ads_order_count:
            raise ValueError(
                "ADS销售指标订单数量核对失败："
                f"源订单数={source_order_count}，"
                f"ADS订单数={ads_order_count}"
            )

        # =====================
        # 6.核对销售金额
        # =====================

        source_sales_amount = (
            valid_order_df["payable_amount"]
            .sum()
        )

        ads_sales_amount = (
            sales["sales_amount"]
            .sum()
        )

        amount_difference = abs(
            source_sales_amount
            - ads_sales_amount
        )

        logger.info(
            f"ADS销售指标金额核对："
            f"源金额={source_sales_amount:.2f}，"
            f"ADS金额={ads_sales_amount:.2f}"
        )

        if amount_difference > 0.01:
            raise ValueError(
                "ADS销售指标金额核对失败："
                f"金额差异={amount_difference:.2f}"
            )

        # =====================
        # 7.保存结果
        # =====================

        self.save(
            sales,
            output
        )

        logger.info(
            f"销售指标完成：{output}，"
            f"统计日期数：{len(sales)}"
        )

        return sales



    def build_user_summary(
            self,
            dws_user_file,
            output
    ):
        logger.info(
            "开始生成ADS用户指标"
        )

        # =====================
        # 1.读取DWS用户主题
        # =====================

        user = self.read(
            dws_user_file
        )

        logger.info(
            f"DWS用户主题读取完成，"
            f"用户数：{len(user)}"
        )

        # =====================
        # 2.检查必要字段
        # =====================

        required_columns = [
            "customer_id",
            "customer_name",
            "order_count",
            "total_amount",
            "avg_amount"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in user.columns
        ]

        if missing_columns:
            raise ValueError(
                "DWS用户主题缺少必要字段："
                f"{missing_columns}"
            )

        # =====================
        # 3.保证指标为数值类型
        # =====================

        user["order_count"] = pd.to_numeric(
            user["order_count"],
            errors="coerce"
        ).fillna(0)

        user["total_amount"] = pd.to_numeric(
            user["total_amount"],
            errors="coerce"
        ).fillna(0)

        user["avg_amount"] = pd.to_numeric(
            user["avg_amount"],
            errors="coerce"
        ).fillna(0)

        # =====================
        # 4.计算用户等级
        # =====================

        def get_user_level(total_amount):
            if total_amount >= 10000:
                return "VIP"
            elif total_amount >= 5000:
                return "重点用户"
            else:
                return "普通用户"

        user["user_level"] = (
            user["total_amount"]
            .apply(get_user_level)
        )

        # =====================
        # 5.检查用户等级
        # =====================

        level_count = (
            user["user_level"]
            .value_counts()
        )

        logger.info(
            "用户等级分布：\n"
            f"{level_count}"
        )

        if user["user_level"].isna().any():
            raise ValueError(
                "ADS用户指标存在未分级用户"
            )

        # =====================
        # 6.保存ADS
        # =====================

        self.save(
            user,
            output
        )

        logger.info(
            f"用户指标保存完成：{output}，"
            f"用户数：{len(user)}"
        )

        return user

    def build_product_summary(
            self,
            dws_product_file,
            output
    ):
        logger.info(
            "开始生成ADS商品指标"
        )

        # =====================
        # 1.读取DWS商品主题
        # =====================

        product = self.read(
            dws_product_file
        )

        logger.info(
            f"DWS商品主题读取完成，"
            f"商品数：{len(product)}"
        )

        # =====================
        # 2.检查必要字段
        # =====================

        required_columns = [
            "product_id",
            "product_name",
            "category_name",
            "sales_count",
            "sales_amount"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in product.columns
        ]

        if missing_columns:
            raise ValueError(
                "DWS商品主题缺少必要字段："
                f"{missing_columns}"
            )

        # =====================
        # 3.转换指标类型
        # =====================

        product["sales_count"] = pd.to_numeric(
            product["sales_count"],
            errors="coerce"
        ).fillna(0)

        product["sales_amount"] = pd.to_numeric(
            product["sales_amount"],
            errors="coerce"
        ).fillna(0)

        # =====================
        # 4.计算销量排名
        # =====================

        product["sales_count_rank"] = (
            product["sales_count"]
            .rank(
                method="dense",
                ascending=False
            )
            .astype(int)
        )

        # =====================
        # 5.计算销售额排名
        # =====================

        product["sales_amount_rank"] = (
            product["sales_amount"]
            .rank(
                method="dense",
                ascending=False
            )
            .astype(int)
        )

        # 默认按销售额排名展示
        product = (
            product.sort_values(
                by=[
                    "sales_amount_rank",
                    "sales_count_rank",
                    "product_id"
                ],
                ascending=[
                    True,
                    True,
                    True
                ]
            )
            .reset_index(drop=True)
        )

        # =====================
        # 6.核对商品数量
        # =====================

        source_product_count = len(product)

        ranked_product_count = (
            product["sales_amount_rank"]
            .notna()
            .sum()
        )

        logger.info(
            f"ADS商品数量核对："
            f"DWS商品数={source_product_count}，"
            f"完成排名商品数={ranked_product_count}"
        )

        if source_product_count != ranked_product_count:
            raise ValueError(
                "ADS商品排名数量核对失败"
            )

        # =====================
        # 7.保存ADS
        # =====================

        self.save(
            product,
            output
        )

        logger.info(
            f"商品指标保存完成：{output}，"
            f"商品数：{len(product)}"
        )

        return product

    def build_user_rfm_base(self,
                            dws_user_file,
                            output):
        logger.info(
            '开始生成用户RFM基础指标'
        )
        # =====================
        # 1.读取DWS用户主题
        # =====================

        user = self.read(
            dws_user_file
        )
        required_columns = [
            "customer_id",
            "customer_name",
            "order_count",
            "total_amount",
            "last_order_time"
        ]
        missing_columns = [
            column
            for column in required_columns
            if column not in user.columns
        ]

        if missing_columns:
            raise ValueError(
                "DWS用户主题缺少RFM必要字段："
                f"{missing_columns}"
            )
        # =====================
        # 2.转换字段类型
        # =====================

        user["last_order_time"] = pd.to_datetime(
            user["last_order_time"],
            errors="coerce"
        )

        user["order_count"] = pd.to_numeric(
            user["order_count"],
            errors="coerce"
        ).fillna(0)

        user["total_amount"] = pd.to_numeric(
            user["total_amount"],
            errors="coerce"
        ).fillna(0)

        # =====================
        # 3.检查无效时间
        # =====================

        invalid_time_count = (
            user["last_order_time"]
            .isna()
            .sum()
        )

        logger.info(
            f"用户最近购买时间无效数："
            f"{invalid_time_count}"
        )

        if invalid_time_count > 0:
            raise ValueError(
                "存在无法解析的用户最近购买时间，"
                f"数量：{invalid_time_count}"
            )
        # =====================
        # 4.确定分析基准日期
        # =====================

        analysis_date = (
                user["last_order_time"]
                .max()
                .normalize()
                +
                pd.Timedelta(days=1)
        )

        logger.info(
            f"RFM分析基准日期："
            f"{analysis_date:%Y-%m-%d}"
        )

        # =====================
        # 5.计算RFM基础指标
        # =====================

        rfm = user[
            [
                "customer_id",
                "customer_name"
            ]
        ].copy()

        rfm["recency_days"] = (
                analysis_date
                -
                user["last_order_time"]
                .dt.normalize()
        ).dt.days

        rfm["frequency"] = (
            user["order_count"]
        )

        rfm["monetary"] = (
            user["total_amount"]
        ).round(2)

        rfm["analysis_date"] = (
            analysis_date
        )

        # =====================
        # 6.基础业务校验
        # =====================

        if (
                rfm["recency_days"] < 0
        ).any():
            raise ValueError(
                "RFM出现负的最近购买间隔"
            )

        if (
                rfm["frequency"] <= 0
        ).any():
            raise ValueError(
                "RFM出现购买次数小于等于0的用户"
            )

        if (
                rfm["monetary"] <= 0
        ).any():
            raise ValueError(
                "RFM出现消费金额小于等于0的用户"
            )

        # =====================
        # 7.核对用户数和金额
        # =====================

        source_user_count = len(user)
        rfm_user_count = len(rfm)

        source_amount = (
            user["total_amount"].sum()
        )

        rfm_amount = (
            rfm["monetary"].sum()
        )

        logger.info(
            f"RFM用户数核对："
            f"DWS用户数={source_user_count}，"
            f"RFM用户数={rfm_user_count}"
        )

        logger.info(
            f"RFM金额核对："
            f"DWS金额={source_amount:.2f}，"
            f"RFM金额={rfm_amount:.2f}"
        )

        if source_user_count != rfm_user_count:
            raise ValueError(
                "RFM用户数量与DWS不一致"
            )

        if abs(
                source_amount
                -
                rfm_amount
        ) > 0.01:
            raise ValueError(
                "RFM用户金额与DWS不一致"
            )

        # =====================
        # 8.输出指标范围
        # =====================

        logger.info(
            "RFM基础指标统计：\n"
            f"{rfm[['recency_days', 'frequency', 'monetary']].describe()}"
        )

        # =====================
        # 9.保存结果
        # =====================

        self.save(
            rfm,
            output
        )

        logger.info(
            f"用户RFM基础指标完成："
            f"{output}，"
            f"用户数={len(rfm)}"
        )

        return rfm

    def calculate_percentile_score(
            self,
            series,
            higher_is_better=True
    ):
        """
        将指标按照百分位转换为1～5分。

        higher_is_better=True：
            数值越大，得分越高。
            用于F和M。

        higher_is_better=False：
            数值越小，得分越高。
            用于R。
        """

        percentile_rank = (
            series.rank(
                method="average",
                pct=True,
                ascending=True
            )
        )

        score = (
            np.ceil(
                percentile_rank * 5
            )
            .astype(int)
            .clip(1, 5)
        )

        if not higher_is_better:
            score = (
                    6 - score
            )

        return score

    def build_user_rfm_segment(
            self,
            rfm_base_file,
            output,
            summary_output
    ):
        logger.info(
            "开始生成RFM用户评分与分群"
        )

        # =====================
        # 1.读取RFM基础指标
        # =====================

        rfm = self.read(
            rfm_base_file
        )

        required_columns = [
            "customer_id",
            "customer_name",
            "recency_days",
            "frequency",
            "monetary",
            "analysis_date"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in rfm.columns
        ]

        if missing_columns:
            raise ValueError(
                "RFM基础表缺少必要字段："
                f"{missing_columns}"
            )

        # =====================
        # 2.转换数值类型
        # =====================

        for column in [
            "recency_days",
            "frequency",
            "monetary"
        ]:
            rfm[column] = pd.to_numeric(
                rfm[column],
                errors="coerce"
            )

        invalid_count = (
            rfm[
                [
                    "recency_days",
                    "frequency",
                    "monetary"
                ]
            ]
            .isna()
            .any(axis=1)
            .sum()
        )

        if invalid_count > 0:
            raise ValueError(
                "RFM基础指标存在无效值，"
                f"数量：{invalid_count}"
            )

        # =====================
        # 3.计算R/F/M评分
        # =====================

        rfm["r_score"] = (
            self.calculate_percentile_score(
                rfm["recency_days"],
                higher_is_better=False
            )
        )

        rfm["f_score"] = (
            self.calculate_percentile_score(
                rfm["frequency"],
                higher_is_better=True
            )
        )

        rfm["m_score"] = (
            self.calculate_percentile_score(
                rfm["monetary"],
                higher_is_better=True
            )
        )

        # =====================
        # 4.生成综合分数
        # =====================

        rfm["rfm_score"] = (
                rfm["r_score"]
                +
                rfm["f_score"]
                +
                rfm["m_score"]
        )

        rfm["rfm_code"] = (
                rfm["r_score"].astype(str)
                +
                rfm["f_score"].astype(str)
                +
                rfm["m_score"].astype(str)
        )

        # =====================
        # 5.判断R/F/M高低
        # =====================

        rfm["r_high"] = (
                rfm["r_score"] >= 4
        )

        rfm["f_high"] = (
                rfm["f_score"] >= 4
        )

        rfm["m_high"] = (
                rfm["m_score"] >= 4
        )

        # =====================
        # 6.划分八类用户
        # =====================

        segment_mapping = {
            (True, True, True): "重要价值用户",
            (True, False, True): "重要发展用户",
            (False, True, True): "重要保持用户",
            (False, False, True): "重要挽留用户",

            (True, True, False): "一般价值用户",
            (True, False, False): "一般发展用户",
            (False, True, False): "一般保持用户",
            (False, False, False): "一般挽留用户"
        }

        rfm["user_segment"] = [
            segment_mapping[
                (
                    r_high,
                    f_high,
                    m_high
                )
            ]
            for r_high, f_high, m_high
            in zip(
                rfm["r_high"],
                rfm["f_high"],
                rfm["m_high"]
            )
        ]

        # 删除中间判断字段
        rfm.drop(
            columns=[
                "r_high",
                "f_high",
                "m_high"
            ],
            inplace=True
        )

        # =====================
        # 7.检查评分范围
        # =====================

        score_columns = [
            "r_score",
            "f_score",
            "m_score"
        ]

        for column in score_columns:
            invalid_score_count = (
                ~rfm[column]
                .between(1, 5)
            ).sum()

            logger.info(
                f"{column}非法评分数量："
                f"{invalid_score_count}"
            )

            if invalid_score_count > 0:
                raise ValueError(
                    f"{column}存在1～5之外的评分，"
                    f"数量：{invalid_score_count}"
                )

        # =====================
        # 8.检查综合分数
        # =====================

        invalid_total_score_count = (
            ~rfm["rfm_score"]
            .between(3, 15)
        ).sum()

        logger.info(
            f"RFM综合分数非法数量："
            f"{invalid_total_score_count}"
        )

        if invalid_total_score_count > 0:
            raise ValueError(
                "RFM综合分数存在3～15之外的值，"
                f"数量：{invalid_total_score_count}"
            )

        # =====================
        # 9.检查用户分类
        # =====================

        segment_null_count = (
            rfm["user_segment"]
            .isna()
            .sum()
        )

        logger.info(
            f"RFM未分类用户数："
            f"{segment_null_count}"
        )

        if segment_null_count > 0:
            raise ValueError(
                "RFM存在未完成分类的用户，"
                f"数量：{segment_null_count}"
            )
        valid_segments = {
            "重要价值用户",
            "重要发展用户",
            "重要保持用户",
            "重要挽留用户",
            "一般价值用户",
            "一般发展用户",
            "一般保持用户",
            "一般挽留用户"
        }

        invalid_segment_count = (
            ~rfm["user_segment"]
            .isin(valid_segments)
        ).sum()

        if invalid_segment_count > 0:
            raise ValueError(
                "RFM出现未定义的用户分类，"
                f"数量：{invalid_segment_count}"
            )
        # =====================
        # 10.核对用户数量
        # =====================

        rfm_user_count = len(rfm)

        segment_user_count = (
            rfm["user_segment"]
            .value_counts(
                dropna=False
            )
            .sum()
        )

        logger.info(
            f"RFM用户数量核对："
            f"基础用户数={rfm_user_count}，"
            f"完成分群用户数={segment_user_count}"
        )

        if rfm_user_count != segment_user_count:
            raise ValueError(
                "RFM分群用户数量不一致"
            )
        # =====================
        # 11.输出分群分布
        # =====================

        segment_summary = (
            rfm.groupby(
                "user_segment",
                as_index=False
            )
            .agg(
                user_count=(
                    "customer_id",
                    "nunique"
                ),
                avg_recency=(
                    "recency_days",
                    "mean"
                ),
                avg_frequency=(
                    "frequency",
                    "mean"
                ),
                total_monetary=(
                    "monetary",
                    "sum"
                ),
                avg_monetary=(
                    "monetary",
                    "mean"
                )
            )
        )

        segment_summary[
            "user_rate"
        ] = (
                segment_summary["user_count"]
                /
                segment_summary["user_count"].sum()
                *
                100
        ).round(2)

        segment_summary[
            "avg_recency"
        ] = (
            segment_summary["avg_recency"]
            .round(2)
        )

        segment_summary[
            "avg_frequency"
        ] = (
            segment_summary["avg_frequency"]
            .round(2)
        )

        segment_summary[
            "total_monetary"
        ] = (
            segment_summary["total_monetary"]
            .round(2)
        )

        segment_summary[
            "avg_monetary"
        ] = (
            segment_summary["avg_monetary"]
            .round(2)
        )

        logger.info(
            "RFM用户分群统计：\n"
            f"{segment_summary.to_string(index=False)}"
        )

        # =====================
        # 12.金额检查
        # =====================

        rfm_total_amount = (
            rfm["monetary"]
            .sum()
        )

        logger.info(
            f"RFM分群消费金额合计："
            f"{rfm_total_amount:.2f}"
        )

        if rfm_total_amount <= 0:
            raise ValueError(
                "RFM分群消费金额合计异常"
            )
        # =====================
        # 13.保存结果
        # =====================

        self.save(
            rfm,
            output
        )

        self.save(
            segment_summary,
            summary_output
        )

        logger.info(
            f"RFM用户分群完成："
            f"{output}，"
            f"用户数={len(rfm)}"
        )

        logger.info(
            f"RFM分群汇总完成："
            f"{summary_output}，"
            f"分群数={len(segment_summary)}"
        )

        return rfm

        return rfm




