import re
import os
from pyspark import SparkContext
from pyspark.sql import *
import pyspark.sql.functions as psf
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException

# from datetime import datetime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


def subtitles_to_string(subtitles):
    result = ""
    for sentence in subtitles:
        for word in sentence:
            if re.match("^[a-zA-Z]+$", word):
                result = result + " " + word
            else:
                result += word
        result += "\n"
    return result


udf_subtitles_to_string = psf.udf(subtitles_to_string, StringType())


df_films = spark.read.parquet("films2.parquet")

df_films_strings = df_films.withColumn("text", udf_subtitles_to_string("subtitles")).drop("subtitles")
df_films_strings.write.mode("overwrite").parquet("filmtext.parquet")
