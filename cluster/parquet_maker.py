import re
import os
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException

# from datetime import datetime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)



def unionAll(*dfs):
    first, rest = dfs[0], dfs[1:]  # Python 3.x, for 2.x you'll have to unpack manually
    return first.sql_ctx.createDataFrame(
        first.sql_ctx._sc.union([df.rdd for df in dfs]),
        first.schema
    )
parquet_list = []

parquet_names = ["1946", "1958", "1965", "1970", "1982",
                 "1989", "1991", "1996", "2000", "2006",
                 "2007", "2008", "2011", "2015", "2016", "2018"]
for parquet in parquet_names:
    parquet_list.append(spark.read.parquet("final/" + parquet + ".parquet"))

df = unionAll(*parquet_list)
df_result = df.groupBy("tconst").agg({"num_subtitles": "max"}).withColumnRenamed("max(num_subtitles)", "num_subtitles")
df_final = df_result.join(df, ["tconst", "num_subtitles"]).drop("max(num_subtitles)", "sid").withColumnRenamed("num_subtitles", "num_sentences")
df_final.write.parquet("big_data.parquet")