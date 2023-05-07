from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.window import Window


spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()

print('title.basics')
df_basics = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)
df_basics.show(1)
df_episode = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.episode.tsv.gz', header=True)
df_episode.show(1)

Query_6_episodeNumber = df_episode\
.groupBy("tconst") \
.agg(count("episodeNumber").alias("episodeNumber_1")) \
.withColumnRenamed("count(episodeNumber)", "num_episodeNumber")
Query_6_episodeNumber.show(5)

Query_6_episodeNumber_movies = Query_6_episodeNumber.join(df_basics, Query_6_episodeNumber.tconst == df_basics.tconst)
cols = ["episodeNumber_1","primaryTitle","genres"]
Query_6_episodeNumber_movies.select(cols).show(10)


