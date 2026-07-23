import os
from utils.reader import read_csv_with_schema


class BaseBuilder:

    def read(self,file,schema=None):
        return read_csv_with_schema(file,schema)

    def save(self,df,output_file):
        output_dir = os.path.dirname(output_file)

        if  output_dir:
            os.makedirs(
                output_dir,
                exist_ok=True
            )
        df.to_csv(
            output_file,
            index=False,
            encoding='utf-8-sig'
        )
