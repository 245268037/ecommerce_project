from .base_rule import BaseRule

class DuplicateRule(BaseRule):
    def __init__(self,key):
        self.key = key

    def check(self, df):
        return df[df.duplicated(self.key)]