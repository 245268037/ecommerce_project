import pandas as pd


class DataLoader:


    def load_data(self,path):
        data = pd.read_csv(path)
        return data