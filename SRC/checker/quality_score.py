from utils.logger import logger


class QualityScore:
    def __init__(self):
        self.weights = {
            "NullRule":30,
            "DuplicateRule":20,
            "TypeRule":20,
            "BusinessRule":20,
            "AmountRule":10
        }

    def calculate(self,results):
        total_score = 100
        detail = []

        for rule_name,result in results.items():
            weight  = self.weights.get(
                rule_name,
                0
            )
            if result.status == 'SUCCESS':
                score = weight
            else:
                score = max(
                    0,
                    weight-result.error_count * 0.1
                )
        total_score -= (
                weight - score
        )
        detail.append(
            {"rule_name":rule_name,
                "score":round(
                    score,
                    2
                ),
                "weight":weight,
                "status":result.status,
                "error_count":result.error_count,
                'severity':result.severity,
             }
        )
        return {
            "total_score":
                round(total_score, 2),
            "detail":
                detail,
            "level":
                self.level(total_score)
        }

    def level(self, score):

        if score >= 90:
            return "A级"

        elif score >= 80:
            return "B级"

        elif score >= 60:
            return "C级"

        else:
            return "D级"


