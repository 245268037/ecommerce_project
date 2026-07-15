import pandas as pd

def read_csv_with_schema(
        path,
        schema=None
):

    if schema:

        return pd.read_csv(
            path,
            dtype=schema
        )

    else:

        return pd.read_csv(path)