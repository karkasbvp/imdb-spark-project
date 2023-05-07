from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()

print('title.basics')
df_basics = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)
df_basics.show(1)
#Query_6 = df_basics.select('primaryTitle','genres').show()
df_ratings = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.ratings.tsv.gz', header=True)
df_ratings.show(1)

Query_8_most_popular = df_ratings\
.groupBy("tconst")\
.agg(count("numVotes"))\
.withColumnRenamed("count(userId)", "num_ratings")
Query_8_most_popular.show()

Query_8_most_popular_movies = Query_8_most_popular.join(df_basics, Query_8_most_popular.tconst == df_basics.tconst)
Query_8_most_popular_movies.show(10, truncate=False)

#   top_rated movies only showing top 10
Query_8_top_rated = df_ratings\
.groupBy("tconst")\
.agg(avg(col("averageRating")))\
.withColumnRenamed("avg(rating)", "avg_rating")\
.sort(desc("avg(averageRating)"))
Query_8_top_rated.show(10)
#  showing movies with title
Query_8_most_popular_movies = Query_8_top_rated.join(df_basics, Query_8_top_rated.tconst == df_basics.tconst)

cols = ["primaryTitle","genres"]
Query_8_most_popular_movies.select(cols).show(10)
