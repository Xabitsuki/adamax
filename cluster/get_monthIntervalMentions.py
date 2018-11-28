import os
from pyspark.sql.types import *
import datetime as dt
from pyspark.sql.functions import *
from pyspark.sql import *


spark = SparkSession.builder.getOrCreate()


MENTIONS_SCHEMA = StructType([
    StructField("GLOBALEVENTID",LongType(),True),
    StructField("EventTimeDate",LongType(),True),
    StructField("MentionTimeDate",LongType(),True),
    StructField("MentionType",LongType(),True),
    StructField("MentionSourceName",StringType(),True),
    StructField("MentionIdentifier",StringType(),True),
    StructField("SentenceID",LongType(),True),
    StructField("Actor1CharOffset",LongType(),True),
    StructField("Actor2CharOffset",LongType(),True),
    StructField("ActionCharOffset",LongType(),True),
    StructField("InRawText",LongType(),True),
    StructField("Confidence",LongType(),True),
    StructField("MentionDocLen",LongType(),True),
    StructField("MentionDocTone",FloatType(),True),
    StructField("MentionDocTranslationInfo",StringType(),True),
    StructField("Extras",StringType(),True)
    ])

DATA_DIR = "hdfs:///datasets/gdeltv2"
mentions = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.mentions.CSV"),schema=MENTIONS_SCHEMA)
mentions.registerTempTable('mentions_table')


query_mentions_fr= """ 
select GLOBALEVENTID, MentionTimeDate, MentionSourceName, MentionDocTone 
from mentions_table 
where MentionSourceName like '%.fr'"""

fr_mentions = spark.sql(query_mentions_fr)

fr_mentions = fr_mentions.withColumn("Year-Month-Day", to_date(concat(col("MentionTimeDate")), "yyyyMMddhhmmss"))

fr_mentions = fr_mentions.select('MentionDocTone', 'MentionTimeDate', 'MentionSourceName', 'Year-Month-Day') \
                                  .withColumn('Month', month(fr_mentions["Year-Month-Day"]))

fr_mentions.write.parquet('fr_mentions_casted_date.parquet', mode='overwrite')



query_mentions_it= """ 
select GLOBALEVENTID, MentionTimeDate, MentionSourceName, MentionDocTone 
from mentions_table 
where MentionSourceName like '%.it'"""

it_mentions = spark.sql(query_mentions_it)

it_mentions = it_mentions.withColumn("Year-Month-Day", to_date(concat(col("MentionTimeDate")), "yyyyMMddhhmmss"))

it_mentions = it_mentions.select('MentionDocTone', 'MentionTimeDate', 'MentionSourceName', 'Year-Month-Day') \
                                  .withColumn('Month', month(it_mentions["Year-Month-Day"]))

it_mentions.write.parquet('it_mentions_casted_date.parquet', mode='overwrite')
