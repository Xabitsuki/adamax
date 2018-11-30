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

def load_df(path):
    """Load an XML subtitles file into a dataframe"""
    df_film = sqlContext.read.format('com.databricks.spark.xml')\
                             .options(rowTag='document')\
                             .load(path)
    return df_film

def get_paths(command_ar) :
     # ls in year file
    out_ls = subprocess.check_output(command_ar)
    # recover lines format
    out_ls = out_ls.split('\n')
    # drop first and last elements
    out_ls = out_ls[1:-1]
    # get full path to movies: in output of ls, only the
    # last element (separated by spaces) is the path
    paths = [(path.split(' ')[-1]).split('/')[-1] for path in out_ls]
    return paths

def run():
    # schema_films = StructType([StructField('tconst', StringType(), False),
    #                            StructField('num_subtitles', LongType(), True),
    #                            StructField('year', LongType(), True),
    #                            StructField('blocks', LongType(), True),
    #                            StructField('subtitle_mins', DoubleType(), True),
    #                            StructField('genres', ArrayType(StringType()), True),
    #                            StructField('subtitles', ArrayType(ArrayType(StringType())), True)])
    # Create empty dataframe with specified schema
    # df_films = spark.createDataFrame([], schema_films)
    years = map(lambda x: str(x), range(1920, 2019))
    path = '/datasets/opensubtitle/OpenSubtitles2018/xml/en/'

    # for all years
    for year in years:
        # Retrieve path to the movies
        year_path = path + year
        movie_ids = get_paths(['hadoop','fs','-ls', year_path])
        # Keep only 7-character movie ids
        # movie_paths = filter(lambda path : len(path.split('/')[-1]) == 7, movie_paths)
        print(year_path)
        for movie_id in movie_ids:
            movie_path = year_path + "/" + movie_id
            files = get_paths(['hadoop','fs','-ls', movie_path])
            print(movie_path)
            for fn in files:
                # TODO
                file_path = movie_path + "/" + fn
                # Create a dataframe for each file
                # df_document = load_df(file_path)
                # Restructure dataframe and add it to df_films
                # df_films = df_films.unionAll(clean_df(df_document, imdb_id))
                print(file_path)


if __name__ == '__main__':
    run()
