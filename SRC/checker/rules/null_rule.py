import pandas as pd
from .base_rule import BaseRule

class NullRule(BaseRule):

    def check(self,df):
        reprot = pd.DataFrame({
            '字段':df.columns,
            '空值数量':df.isnull().sum().values,
            '空值率':(df.isnull().mean()*100).round(2)
        })
        return reprot