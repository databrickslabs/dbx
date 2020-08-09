import json
import os
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from logging import Logger
from typing import Dict, Any

from pyspark.sql import SparkSession


# abstract class for jobs
class Job(ABC):
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.logger = self.__prepare_logger()

        self.__define_config_source()

        self.__log_conf()

    def __define_config_source(self):
        if os.environ.get("DBX_AUTOMATED_EXECUTION"):
            self.logger.info("Reading configuration from --conf-file job option")
            conf_file = self.__get_conf_file()
            self.conf = self.__read_config(conf_file)
        else:
            self.conf = self.dev_config

    @staticmethod
    def __get_conf_file():
        p = ArgumentParser()
        p.add_argument("--conf-file", required=True, type=str)
        namespace = p.parse_args()
        return namespace.conf_file

    def __read_config(self, conf_file) -> Dict[str, Any]:
        raw_content = "".join(self.spark.read.format("text").load(conf_file).toPandas()["value"].tolist())
        config = json.loads(raw_content)
        return config

    def __prepare_logger(self) -> Logger:
        log4j_logger = self.spark._jvm.org.apache.log4j
        logger = log4j_logger.LogManager.getLogger(self.__class__.__name__)
        return logger

    def __log_conf(self):
        # log parameters
        self.logger.info("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info("\t Parameter: %-30s with value => %-30s" % (key, item))

    @abstractmethod
    def launch(self):
        pass

    @property
    @abstractmethod
    def dev_config(self):
        pass
