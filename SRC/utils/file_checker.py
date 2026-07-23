import os

class FileChecker:

    @staticmethod
    def check_exists(file):
        if not os.path.exists(file):
            raise FileNotFoundError(f'{file}不存在')


    @staticmethod
    def check_empty(file):
        if os.path.getsize(file) == 0:
            raise Exception(f"{file}为空")


    @staticmethod
    def check_size(file):

        size = os.path.getsize(file)
