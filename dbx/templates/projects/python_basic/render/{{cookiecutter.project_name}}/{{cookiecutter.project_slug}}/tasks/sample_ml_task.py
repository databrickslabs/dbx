from {{cookiecutter.project_slug}}.common import Task
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
import pandas as pd
import mlflow.sklearn
import mlflow

class SampleMLTask(Task):
    TARGET_COLUMN: str = "MedHouseVal"

    def _read_data(self) -> pd.DataFrame:
        db = self.conf["input"].get("database", "default")
        table = self.conf["input"]["table"]
        self.logger.info(f"Reading housing dataset from {db}.{table}")
        _data: pd.DataFrame = self.spark.table(f"{db}.{table}").toPandas()
        self.logger.info(f"Loaded dataset, total size: {len(_data)}")
        return _data

    @staticmethod
    def _get_pipeline() -> Pipeline:
        pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ('random_forest', RandomForestRegressor())
        ])
        return pipeline

    def _train_model(self):
        mlflow.sklearn.autolog()
        pipeline = self._get_pipeline()
        data = self._read_data()
        X = data.drop(self.TARGET_COLUMN, axis=1)
        y = data[self.TARGET_COLUMN]
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)
        r2_result = r2_score(y_test, y_pred)
        mlflow.log_metric("r2", r2_result)

    def launch(self):
        self.logger.info("Launching sample ETL job")
        mlflow.set_experiment(self.conf["experiment"])
        self._train_model()
        self.logger.info("Sample ETL job finished!")


def entrypoint():  # pragma: no cover
    task = SampleMLTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
