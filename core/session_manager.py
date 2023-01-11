from pyspark.sql import SparkSession


class SessionManager:
    def __init__(self, session_name):
        self.__session_name = session_name

    def __enter__(self):
        self.session = SparkSession.builder.master("local").appName(self.__session_name).getOrCreate()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.stop()
