from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as f

spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()
print('title.episode')
df_episode = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.episode.tsv.gz', header=True)
df_episode.show(1)
print('title.basics')
df_basics = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)
df_basics.show(1)

windowSpec = Window.orderBy('averageRating').partitionBy('seasonNumber')

df_episode.join(df_basics,'tconst').filter(f.col('episodeNumber') != '\\N') \
    .select('primaryTitle','episodeNumber') \
    .withColumn('title',f.split(df_basics['primaryTitle'], '/.').getItem(0)) \
    .orderBy('episodeNumber',ascending=False).show()

#windowSpec = Window.orderBy('averageRating').partitionBy('decades')

df_episode.withColumn('titleSum',f.count(f.col('episodeNumber')).over(windowSpec)).show()

#withColumn('title', f.split(df_basics['primaryTitle'], '/.').getItem(0)).
