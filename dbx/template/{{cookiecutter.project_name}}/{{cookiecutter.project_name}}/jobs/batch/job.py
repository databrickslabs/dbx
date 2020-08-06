import pyspark.sql.functions as F

from dbx.common import Job


class BatchJob(Job):
    def launch(self):
        self.logger.info("Launching the batch job")
        source = (
            self.spark
                .range(0, 1000).toDF()
                .withColumn("value", F.rand())
        )
        source.show(self.conf["num_rows"])
        self.logger.info("Batch job finished")


if __name__ == '__main__':
    batch_job = BatchJob()
    batch_job.launch()
