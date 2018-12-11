import re
import os
from pyspark import SparkContext
from pyspark.sql import *
import pyspark.sql.functions as psf
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from sacremoses import MosesDetokenizer

# from datetime import datetime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

def subtitles_to_string(subtitles):
    result = ""
    for subtitle in subtitles:
        test = MosesDetokenizer().detokenize(subtitle)
        result += test + "\n"
    return result


udf_subtitles_to_string = psf.udf(subtitles_to_string, StringType())


df_films = spark.read.parquet("films2.parquet")

df_films_strings = df_films.withColumn("text", udf_subtitles_to_string("subtitles")).drop("subtitles")
df_films_strings.write.mode("overwrite").parquet("filmtext.parquet")
