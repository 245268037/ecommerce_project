from warehouse.base_builder import BaseBuilder
from config.settings import SOURCE_SYSTEM
from datetime import datetime
from utils.logger import logger

class ODSBuilder(BaseBuilder):
    def build_ods(
            self,
            input_file,
            output_file,
            schema
    ):
        logger.info(
            f"开始构建ODS:{input_file}"
        )
         # =====================
        # 1.读取数据
        # =====================

        df = self.read(
            input_file,
            schema
        )

        # 2.增加ETL字段
        etl_time = datetime.now()
        df["etl_time"] = etl_time
        df["source_system"] = SOURCE_SYSTEM
        df["etl_batch"] = (
            etl_time.strftime(
                "%Y%m%d%H%M%S"
            )
        )

        # 3.保存ODS
        self.save(
            df,
            output_file
        )
        logger.info(f'ODS构建完成: {output_file}')
        return df