import pandas as pd

class QualityChecker:


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


    #
    def check_enun(self,dif,col:str,alliwed:list[str]) -> pd.DataFrame:
        #如果传入的字段不在表里面，则返回正常数据
        if col not in dif.columns:
            return pd.DataFrame()

        #取反，只区异常数据
        mask = ~dif[col].isin(alliwed)
        return dif[mask].copy()

    def check_range(self,dif):
        age = dif[~dif['age'].between(0,120)]
        return age

    def check_business(self,dif):
        business = dif.query('not payable_amount != order_amount-coupon_amount+freight_amount')
        return business

#检查有哪些重复数据
    def check_duplicate(self,dif):
        duplicate = dif[dif.duplicated()]
        return duplicate