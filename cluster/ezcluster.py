from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext

# create the session
spark = SparkSession.builder.getOrCreate()
# create the context
sc = spark.sparkContext

# libraries that work

import os
import re

def run():
    print('ca marche bro')


if __name__ == '__main__':
    run()