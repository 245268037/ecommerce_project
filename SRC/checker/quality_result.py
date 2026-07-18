from openpyxl.formatting import rule

from checker.severity import Severity

class QualityResult:
    def __init__(self,
                 rule_name,
                 status,
                 error_count,
                 data=None,
                 message=None
                 ):
        self.rule_name = rule_name
        self.status = status
        self.error_count = error_count
        self.data = data
        self.message = message
        self.severity = Severity.level(
            rule_name
        )

    def to_dict(self):
        return {
            'rule_name':self.rule_name,
            'status':self.status,
            'error_count':self.error_count,
            'data':self.data,
            'message':self.message
        }