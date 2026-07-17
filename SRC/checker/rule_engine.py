class RuleEngine:
    def __init__(self):
        self.rules = []

    def register(self,rule):
        self.rules.append(rule)

    def run(self,df):
        result = {}
        for rule in self.rules:
            result[type(rule).__name__] = rule.check(df)
        return result