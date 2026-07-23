
import pandas as pd

class QualityChecker:

    # 空值检查
    def check_null(self,dif) -> pd.DataFrame:
        #计算空值总共有多少
        null_df = dif.isnull().sum()
        #计算比例
        null_ratio = (dif.isnull().mean()*100).round(2)
        #形成DATAframe文件
        report = pd.DataFrame(
            {'字段': null_df.index,
             '空值数量': null_df.values,
             '空值率': null_ratio.values,
             }
        )
        return report


    # 枚举检查
    def check_enum(self,df,col:str,allowed:list) -> pd.DataFrame:
        #如果传入的字段不在表里面，则返回正常数据
        if col not in df.columns:
            return pd.DataFrame()
        error_date = df[~df[col].isin(allowed)]
        return error_date

    # 范围检查
    def check_range(self,df,col,min_values,max_values):
        if col not in df.columns:
            return pd.DataFrame()

        result = df[~df[col].between(min_values,max_values)]
        return result

    # 业务规则
    def check_business(self,df):
        error = df[
            df["payable_amount"]!=(df["order_amount"]-df["coupon_amount"]+df["freight_amount"])]
        return error



    #检查重复
    def check_duplicate(self,df,subset=None):
        return df[df.duplicated(subset=subset,keep=False)]

#主键检测
    def check_primary_key(self,df,key):
        duplicate = df[key].duplicated().sum()

        result = pd.DataFrame(
            {
                '字段':[key],
                '重复数据':[duplicate]
            }
        )

#金额异常
    def check_amount(self,df,col='payable_amount'):
        if col not in df.columns:
            return pd.DataFrame()
        result = df[df[col]<0]
        return result


