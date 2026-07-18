from utils.logger import logger


class RuleEngine:
    def __init__(self):
        self.rules = []

    def register(self,rule):
        self.rules.append(rule)

    def run(self,df):
        result = {}
        for rule in self.rules:
            name = rule.__class__.__name__
            logger.info(f'执行规则{name}')
            result[name] = rule.check(df)
        return result