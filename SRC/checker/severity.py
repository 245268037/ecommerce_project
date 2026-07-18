class Severity:


    @staticmethod
    def level(rule_name):


        level_map = {


            "NullRule":
                "HIGH",


            "BusinessRule":
                "HIGH",


            "AmountRule":
                "MEDIUM",


            "DuplicateRule":
                "MEDIUM",


            "TypeRule":
                "HIGH"


        }


        return level_map.get(

            rule_name,

            "LOW"

        )