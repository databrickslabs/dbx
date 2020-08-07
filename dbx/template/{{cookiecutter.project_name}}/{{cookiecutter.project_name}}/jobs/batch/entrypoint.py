import pyspark.sql.functions as F

from {{cookiecutter.project_name}}.jobs.common import Job

class BatchJob(Job):
    def launch(self):
        self.logger.info("Launching the batch job")
        source = (
            self.spark
                .range(0, 1000).toDF("id")
                .withColumn("value", F.rand())
        )
        source.show(10)
        self.logger.info("Batch job finished")


batch_job = BatchJob()
batch_job.launch()
