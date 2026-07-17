import pandas as pd

from config.settings import CSV_ENCODING


def read_csv_with_schema(
        file_path,
        schema=None
):

    """
    统一读取CSV文件

    file_path:
        文件路径

    schema:
        字段类型配置

    """

    df = pd.read_csv(
        file_path,
        dtype=schema,
        encoding=CSV_ENCODING,
        low_memory=False
    )

    return df