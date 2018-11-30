import os
import re
import subprocess
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
# from hdfs import Config

# create the session
spark = SparkSession.builder.getOrCreate()
# create the context
sc = spark.sparkContext
sqlContext = SQLContext(sc)
# libraries that work

# path = 'hdfs:///datasets/opensubtitle/OpenSubtitles2018/xml/en/'

def load_df(path):
    """Load an XML subtitles file into a dataframe"""
    df_film = sqlContext.read.format('com.databricks.spark.xml')\
                             .options(rowTag='document')\
                             .load(path)
    return df_film

def run():
    # client = Config().get_client('dev')
    # files = client.list(path)
    # hadoop = sc._jvm.org.apache.hadoop
    # fs = hadoop.fs.FileSystem
    # conf = hadoop.conf.Configuration()
    # path = hadoop.fs.Path('hdfs:///datasets/opensubtitle/OpenSubtitles2018/xml/en/1996/853497')
    # fs.get(conf).listStatus(path) 561586/6614354.xml.gz
    path = 'hdsf:///datasets/opensubtitle/OpenSubtitles2018/xml/en/'
    path_years = path.getFileSystem(sc.hadoopConfiguration).listFiles(path, true)

    print(type(path_years))
    print(path_years)

    for py in path_years:
        print(py)
    #
    # for f in path_years:
    #     print f.getPath()
    #     df = load_df(f.getPath())
    # df.show()
if __name__ == '__main__':
    run()
