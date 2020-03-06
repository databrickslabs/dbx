import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
if 'spark' not in locals():
    spark = SparkSession.builder.appName('Test').getOrCreate()

df = pd.DataFrame(np.random.randint(0, 1000, size=(5000, 1)), columns=['y'])
df['AccountId'] = np.random.randint(0, 3, size=(5000, 1)).astype(dtype='object')
sparkDf = spark.createDataFrame(df)
print(sparkDf.count())