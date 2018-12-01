import re
import os
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


def run ():
    df_sample_document = sqlContext.read.format('com.databricks.spark.xml')\
                                        .options(rowTag='document') \
                                        .load('hdfs:///datasets/opensubtitle/OpenSubtitles2018/xml/en/2017/7232928/7073324.xml.gz')
    df_sample_document.printSchema()
    df_sample_document.show()


if __name__ =='__main__':
    run()
