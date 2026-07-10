import pandas as pd
import os
from datetime import datetime

class ODSBuilder:

    def build_ods(self,input_path,output_path):
        df = pd.read_csv(input_path)
        #抽取时间
        df['etl_time'] = datetime.now()
        #数据来源字段
        df['source_system'] = 'ECommerce'
        #增加批次号
        batch = datetime.now().strftime('%Y%m%d%H%M')
        df['etl_batch'] = batch
        os.makedirs(
            os.path.dirname(output_path),
            exist_ok = True,
        )
       #自动创建目录
        df.to_csv(output_path,index=False,encoding='utf-8-sig')
        return df
