import json
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from logging import Logger
from typing import Dict, Any

from pyspark.sql import SparkSession


# abstract class for jobs
class Job(ABC):
    def __init__(self):
        conf_file = self.__parse_conf_arg()
        self.spark = SparkSession.builder.getOrCreate()
        self.conf = self.__read_conf(conf_file)
        self.logger = self.__prepare_logger()

    @staticmethod
    def __parse_conf_arg() -> str:
        p = ArgumentParser()
        p.add_argument("--conf-file", required=True, type=str)
        namespace = p.parse_args()
        return namespace.conf_file

    def __read_conf(self, conf_file: str) -> Dict[str, Any]:
        raw_content = "".join(self.spark.read.format("text").load(conf_file).toPandas()["value"].tolist())
        config = json.loads(raw_content)
        return config

    def __prepare_logger(self) -> Logger:
        log4j_logger = self.spark._jvm.org.apache.log4j
        logger = log4j_logger.LogManager.getLogger(self.__class__.__name__)
        return logger

    @abstractmethod
    def launch(self):
        pass
