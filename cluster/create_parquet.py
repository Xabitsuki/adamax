import re
import os
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
# from datetime import datetime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
DATA_DIR = 'hdfs:///datasets/opensubtitle/OpenSubtitles2018/xml/en'

def to_subtitles_array(sentences):
    """Function to map the elements (a struct containing tokens)
    to a list of list of tokens """
    s_list = []
    if sentences is None:
        return s_list
    for words in sentences:
        w_list = []
        if words and "w" in words and words["w"]:
            for w in words["w"]:
                if '_VALUE' in w and w['_VALUE'] and re.match("^[a-zA-Z]+$", w['_VALUE']):
                    w_list.append(w['_VALUE'])
            s_list.append(w_list)
    return s_list

## UDF Functions
# Transform to spark function
udf_subtitles_array = udf(to_subtitles_array, ArrayType(ArrayType(StringType())))
# Convert array of words into a single string
udf_sentence = udf(lambda x: ' '.join(x), StringType())
# Function to split genres
udf_split = udf(str.split, ArrayType(StringType()))

def clean_df(df_document, imdb_id):
    """Restructures and selects the columns of a dataframe of an XML
    file with its corresponding IMDB Id"""
    # Create IMDb ID and subtitles column
    df_film_sentences = df_document.withColumn("tconst", lit("tt" + imdb_id))\
                                   .withColumn("subtitles", udf_subtitles_array("s"))

    # Select metadata and previously created columns
    df_result = df_film_sentences.selectExpr("tconst",
                                             "meta.conversion.sentences as num_subtitles",
                                             "meta.source.genre",
                                             "meta.source.year",
                                             "meta.subtitle.blocks",
                                             "meta.subtitle.duration as subtitle_duration",
                                             "meta.subtitle.language",
                                             "subtitles")
    # Split genre column and convert subtitle duration to seconds
    df_result = df_result.withColumn("genres", udf_split("genre")) \
                         .withColumn("subtitle_mins",
                                     unix_timestamp(df_result.subtitle_duration, "HH:mm:ss,SSS") / 60)
    # Discard redundant columns
    return df_result.select("tconst", "num_subtitles", "year", "blocks", "subtitle_mins", "genres", "subtitles")

def load_df(path):
    """Load an XML subtitles file into a dataframe"""
    df_film = sqlContext.read.format('com.databricks.spark.xml')\
                             .options(rowTag='document')\
                             .load(path)
    return df_film

def is_valid_year(year):
    return len(year) == 4 and 1900 < int(year) and int(year) <= 2019

def is_valid_movie_id(movie_id):
    return len(movie_id) == 7

def is_empty(path):
    return not os.listdir(path)

def df_all_files():
    """Function that returns a dataframe with all the films data
    in a path that has the following subdirectories: year/imdb_id/"""

    schema_films = StructType([StructField('tconst', StringType(), False),
                               StructField('num_subtitles', LongType(), True),
                               StructField('year', LongType(), True),
                               StructField('blocks', LongType(), True),
                               StructField('subtitle_mins', DoubleType(), True),
                               StructField('genres', ArrayType(StringType()), True),
                               StructField('subtitles', ArrayType(ArrayType(StringType())), True)])
    # Create empty dataframe with specified schema
    df_films = spark.createDataFrame([], schema_films)

    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path(DATA_DIR)
    years = map(lambda y: str(y), range(1910, 2019))
    # 1996/853497
    #561586/6614354.xml.gz
    for year in years:
        year_path = hadoop.fs.Path(DATA_DIR + "/" + year)
        # print(year_path)
        for i in fs.get(conf).listStatus(year_path):
            id = str(i.getPath()).split('/')[-1]
            if(len(id) == 7):
                movie_path = hadoop.fs.Path(DATA_DIR + "/" + year + "/" + id)
                count = 0
                for f in fs.get(conf).listStatus(movie_path):
                    if count == 1:
                        break
                    fn = str(f.getPath()).split('/')[-1]
                    file_path = DATA_DIR + "/" + year + "/" + id + "/" + fn
                    # Create a dataframe for each file
                    df_document = load_df(file_path)
                    # Restructure dataframe and add it to df_films
                    df_films = df_films.unionAll(clean_df(df_document, id))
                    count = count + 1
                    # print(fn)
    return df_films

def run():
    df_films = df_all_files()
    print("Create parquet")
    # Create parquet file
    df_films.write.mode('overwrite').parquet("test.parquet")

if __name__ == '__main__':
    run()
