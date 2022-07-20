from {{cookiecutter.project_slug}}.common import Task
from sklearn.datasets import fetch_california_housing
import pandas as pd


class SampleETLTask(Task):
    def _write_data(self):
        db = self.conf["output"].get("database", "default")
        table = self.conf["output"]["table"]
        self.logger.info(f"Writing housing dataset to {db}.{table}")
        _data: pd.DataFrame = fetch_california_housing(as_frame=True).frame
        df = self.spark.createDataFrame(_data)
        df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.{table}")
        self.logger.info("Dataset successfully written")

    def launch(self):
        self.logger.info("Launching sample ETL job")
        self._write_data()
        self.logger.info("Sample ETL job finished!")


def entrypoint():  # pragma: no cover
    task = SampleETLTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
