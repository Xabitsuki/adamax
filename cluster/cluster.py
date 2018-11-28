import os
import re
import findspark
import pandas as pd
findspark.init()
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
# import matplotlib
# import matplotlib.pyplot as plt
# %matplotlib inline
import urllib.request
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell'
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext
sqlContext = SQLContext(sc)


def run ():
    df_sample_document = sqlContext.read.format('com.databricks.spark.xml')\
                                        .options(rowTag='document') \
                                        .load('../sample_dataset/2017/6464116/6887453.xml.gz')
    df_sample_document.printSchema()
    df_sample_document.show()


if __name__ =='__main__':
    run()