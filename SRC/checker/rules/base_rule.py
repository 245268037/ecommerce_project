from abc import ABC,abstractmethod

class BaseRule(ABC):
    """
        数据质量规则基类
    """
    @abstractmethod
    def check(self):
        pass