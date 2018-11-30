from pyspark.sql import SparkSession
from pyspark.sql import SQLContext # if needed

spark = SparkSession.builder.getOrCreate()
## Global variables
# Path
path = '/datasets/opensubtitle/OpenSubtitles2018/xml/en/'
# Year range
years = map(lambda x : str(x), range(1900, 2019))

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
                if '_VALUE' in w and w['_VALUE']:
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

def get_paths(command_ar) :
     # ls in year file
    out_ls = subprocess.check_output(command_ar)
    # recover lines format
    out_ls = out_ls.split('\n')
    # drop first and last elements
    out_ls= out_ls[1:-1]
    # get full path to movies: in output of ls, only the
    # last element (separated by spaces) is the path
    paths = [path.split(' ')[-1] for path in out_ls]
    return paths

def df_all_files(path):
    """Function that returns a dataframe with all the films data
    in a path that has the following subdirectories: year/imdb_id/"""

    # for all years
    for year in years:
        path_to_year = path + year
        # Retrieve path to the movies
        movie_paths = get_paths(['hadoop','fs','-ls', path_to_year])
        # Keep only 7-character movie ids
        movie_paths = filter(lambda path : len(path.split('/')[-1]) == 7, movie_paths)

        if len(path_to_movies) != 0:
            for movie_id in movie_paths:
                movie_path = path_to_year + "/" + movie_id
                file_paths = get_paths(['hadoop','fs','-ls', movie_path])
                for idx, fn in enumerate(file_paths):
                    file_path = file_paths + "/" + fn
                    # Create a dataframe for each file
                    df_document = load_df(file_path)
                    if idx == 0:
                        df_films = clean_df(df_document, imdb_id)
                    else:
                        # Restructure dataframe and add it to df_films
                        df_films = df_films.unionAll(clean_df(df_document, imdb_id))
    return df_films

df_films = df_all_files(path)
# Create parquet file
df_films.write.mode('overwrite').parquet("films.parquet")
