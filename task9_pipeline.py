from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as f
#
spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()
print('title.basics')
df_basics = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)
df_basics.show(1)
Query_9 = df_basics.select('primaryTitle','genres').show()

from pyspark.sql.functions import *
df_ratings = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.ratings.tsv.gz', header=True)
df_ratings.show(1)

# most_popular movies only showing 5
Query_9_most_popular = df_ratings\
.groupBy("tconst")\
.agg(count("numVotes"))\
.withColumnRenamed("count(userId)", "num_ratings")
Query_9_most_popular.show()

Query_10_most_popular_movies = Query_9_most_popular.join(df_basics, Query_9_most_popular.tconst == df_basics.tconst)
Query_10_most_popular_movies.show(5, truncate=False)

#   top_rated movies only showing top 5
Query_11_top_rated = df_ratings\
.groupBy("tconst")\
.agg(avg(col("averageRating")))\
.withColumnRenamed("avg(rating)", "avg_rating")\
.sort(desc("avg(averageRating)"))
Query_11_top_rated.show(5)
#  showing movies with title
Query_12_most_popular_movies = Query_11_top_rated.join(df_basics, Query_11_top_rated.tconst == df_basics.tconst)
Query_12_most_popular_movies.show(5, truncate=False)

# top_rated with numVotes
Query_13_top_rated_numVotes = df_ratings\
.groupBy("tconst")\
.agg(avg(col("averageRating")), count("numVotes")) \
.withColumnRenamed("avg(rating)", "avg_rating_1")\
.withColumnRenamed("count(numVotes)","numVotes_rating")
Query_13_top_rated_numVotes.show(5)

# showing movies with title and nunVotes and ratings
Query_14_top_rated_movies = Query_13_top_rated_numVotes.join(df_basics, Query_13_top_rated_numVotes.tconst == df_basics.tconst).sort(desc("avg(averageRating)"), desc("numVotes_rating"))
Query_14_top_rated_movies.show(10)

# Calculate average, minimum, and maximum of num_ratings
Query_15_statistic = Query_14_top_rated_movies.select([mean('numVotes_rating'), min('numVotes_rating'), max('numVotes_rating')])
Query_15_statistic.show(1)

#
Query_14_top_rated_movies.where("numVotes_rating >= 1").show(5, truncate=False)

# most polarising movies
Query_16_ratings_stddev = df_ratings\
.groupBy("tconst")\
.agg(count("numVotes").alias("num_ratings"),
     avg(col("averageRating")).alias("avg_rating"),
     stddev(col("averageRating")).alias("std_rating")
    )\
.where("num_ratings >= 1")
Query_16_ratings_stddev.show(5)

Query_17_marmite_movies = Query_16_ratings_stddev.join(df_basics, Query_16_ratings_stddev.tconst == df_basics.tconst)
Query_17_marmite_movies.show(5)