from abc import ABC, abstractmethod
from logging import Logger

from pyspark.sql import SparkSession


# abstract class for jobs
class Job(ABC):
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.logger = self.__prepare_logger()

    def __prepare_logger(self) -> Logger:
        log4j_logger = self.spark._jvm.org.apache.log4j
        logger = log4j_logger.LogManager.getLogger(self.__class__.__name__)
        return logger

    @abstractmethod
    def launch(self):
        pass
