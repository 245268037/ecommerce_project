from pipeline.etl_pipeline import ETLPipeline
from utils.logger import logger


def main():
    logger.info(
        '启动电商ETL项目'
    )
    pipeline = ETLPipeline()
    pipeline.run()

if __name__ == '__main__':
    main()