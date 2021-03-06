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
DATA_DIR = 'hdfs:///datasets/opensubtitle/OpenSubtitles2018/xml/en'

imdb_ids = spark.read.csv("ids.csv")
imdb_ids = set(imdb_ids.select('_c1').rdd.map(lambda r: r[0]).collect())


# checks if correct schema
def has_correct_schema(df):
    arguments = [
        "meta.conversion.sentences",
        "meta.source.year",
        "meta.subtitle.blocks",
        "meta.subtitle.duration",
        "meta.subtitle.language",
        "s"]
    for col in arguments:
        try:
            df[col]
        except AnalysisException:
            return False
    return True


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


def clean_df(df_document, imdb_id, count):
    """Restructures and selects the columns of a dataframe of an XML
    file with its corresponding IMDB Id"""
    # Create IMDb ID and subtitles column
    df_film_sentences = df_document.withColumn("tconst", lit("tt" + imdb_id)) \
        .withColumn("subtitles", udf_subtitles_array("s")).withColumn("sid", lit(count))

    # Select metadata and previously created columns
    df_result = df_film_sentences.selectExpr("tconst", "sid",
                                             "meta.conversion.sentences as num_subtitles",
                                             "meta.source.year",
                                             "meta.subtitle.blocks",
                                             "meta.subtitle.duration as subtitle_duration",
                                             "meta.subtitle.language",
                                             "subtitles")
    # Split genre column and convert subtitle duration to seconds
    df_result = df_result.withColumn("subtitle_mins",
                                     unix_timestamp(df_result.subtitle_duration, "HH:mm:ss,SSS") / 60)
    # Discard redundant columns
    return df_result.select("tconst", "sid", "num_subtitles", "year", "blocks", "subtitle_mins", "subtitles")


def load_df(path):
    """Load an XML subtitles file into a dataframe"""
    df_film = sqlContext.read.format('com.databricks.spark.xml') \
        .options(rowTag='document') \
        .load(path)
    return df_film


def is_valid_year(year):
    return len(year) == 4 and 1900 < int(year) and int(year) <= 2019


def is_valid_movie_id(movie_id):
    return len(movie_id) == 7


def is_empty(path):
    return not os.listdir(path)

def unionAll(*dfs):
    first, rest = dfs[0], dfs[1:]  # Python 3.x, for 2.x you'll have to unpack manually
    return first.sql_ctx.createDataFrame(
        first.sql_ctx._sc.union([df.rdd for df in dfs]),
        first.schema
    )
def df_all_files():
    """Function that returns a dataframe with all the films data
    in a path that has the following subdirectories: year/imdb_id/"""

    schema_films = StructType([StructField('tconst', StringType(), False),
                               StructField('sid', IntegerType(), False),
                               StructField('num_subtitles', LongType(), True),
                               StructField('year', LongType(), True),
                               StructField('blocks', LongType(), True),
                               StructField('subtitle_mins', DoubleType(), True),
                               StructField('subtitles', ArrayType(ArrayType(StringType())), True)])
    # Create empty dataframe with specified schema
    film_list = []

    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path(DATA_DIR)
    years = map(lambda y: str(y), range(1910, 2019))
    parquet_list = []
    # 1996/853497
    # 561586/6614354.xml.gz
    for year in years:
        year_path = hadoop.fs.Path(DATA_DIR + "/" + year)
        # print(year_path)
        for i in fs.get(conf).listStatus(year_path):
            id = str(i.getPath()).split('/')[-1]
            if len(id) <= 7:
                imdb_id = id.rjust(7, "0")
            if (("tt" + imdb_id) in imdb_ids):
                movie_path = hadoop.fs.Path(DATA_DIR + "/" + year + "/" + id)
                count = 0
                for f in fs.get(conf).listStatus(movie_path):
                    if count == 4:
                        break
                    fn = str(f.getPath()).split('/')[-1]
                    file_path = DATA_DIR + "/" + year + "/" + id + "/" + fn

                    # Create a dataframe for each file
                    df_document = load_df(file_path)
                    # Restructure dataframe and add it to df_films
                    if (has_correct_schema(df_document)):
                        film_list.append(clean_df(df_document, imdb_id, count))
                        count = count + 1
                    # print(fn)
        if(len(film_list) > 600):
          parquet_file = year + ".parquet"
          parquet_list.append(parquet_file)
          unionAll(*film_list).write.parquet("final/" + parquet_file)
          print("write parquet")
          film_list = []
    if(film_list):
        parquet_file = "2018" + ".parquet"
        parquet_list.append(parquet_file)
        unionAll(*film_list).write.parquet("final/" + parquet_file)


def run():
    df_all_files()
    print("Finished")
    # Create parquet file



if __name__ == '__main__':
    run()