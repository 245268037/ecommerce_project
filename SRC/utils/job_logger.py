import pandas as pd
import os
from datetime import datetime


class JobLogger:
    def __init__(self,file):
        self.file = file

    def write(self,job_name,start,end,status):
        cost = (end - start).seconds

        data = {
            "job_name":job_name,
            "start_time":start,
            "end_time":end,
            "status":status,
            "cost_time":(end-start).seconds
        }
        df = pd.DataFrame([data])
        os.makedirs(os.path.dirname(self.file), exist_ok=True)
        if os.path.exists(self.file):
            old = pd.read_csv(self.file)
            df = pd.concat([old,df])
        df.to_csv(self.file,index=False,encoding="utf-8")