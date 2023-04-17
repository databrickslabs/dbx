import sys

from typing import List

from argparse import ArgumentParser

from mlx_integration_test.common import Task


class ETLTask(Task):
    def _write_data(self):
        self.logger.info("Reading data")
        print("-------------- etl step -----------------")

    def launch(self):
        self.logger.info("Launching sample ETL task")
        print(f"---------------------- {self.__class__.__name__} self.conf -----------------------")
        print(self.conf)
        print("--------------------------------------------------------")
        self._write_data()
        print("--------------- Data written --------------")
        self.logger.info("Sample ETL task finished!")


# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    args = sys.argv
    print("-------------------- TEST ETL TASK ARGS ------------------------")
    print(f"--------  {args} --------")
    print("-----------------------------------------------------------")

    task = ETLTask()
    task.launch()


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
