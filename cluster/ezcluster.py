import os
import re
import subprocess
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext

# create the session
spark = SparkSession.builder.getOrCreate()
# create the context
sc = spark.sparkContext
sqlContext = SQLContext(sc)

def run():
<<<<<<< Updated upstream
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    DATA_DIR = 'hdfs:///datasets/opensubtitle/OpenSubtitles2018/xml/en'
    path = hadoop.fs.Path(DATA_DIR)
    years = map(lambda y: str(y), range(2000, 2019))
    # 1996/853497
    #561586/6614354.xml.gz
    for year in years:
        year_path = hadoop.fs.Path(DATA_DIR + "/" + year)
        # print(year_path)
        for i in fs.get(conf).listStatus(year_path):
            id = str(i.getPath()).split('/')[-1]
            movie_path = hadoop.fs.Path(DATA_DIR + "/" + year + "/" + id)
            for f in fs.get(conf).listStatus(movie_path):
                fn = str(f.getPath()).split('/')[-1]
                print(fn)

if __name__ == '__main__':
    run()
