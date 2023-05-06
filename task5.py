from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()
print('title.akas')
df_akas = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.akas.tsv.gz', header=True)
df_akas.show(1)
print('title.basics')
df_basics = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)
df_basics.show(1)
#print('title.crew')
#df_crew = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.crew.tsv.gz', header=True)
#df_crew.show(1)
#print('title.episode')
#df_episode = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.episode.tsv.gz', header=True)
#df_episode.show(1)
#print('title.principals')
#df_principals = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.principals.tsv.gz', header=True)
#df_principals.show(1)
#print('title.ratings')
#df_ratings = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.ratings.tsv.gz', header=True)
#df_ratings.show(1)
#print('name_basics')
#df_name_basics = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/name.basics.tsv.gz', header=True)
#df_name_basics.show(1)

df_rez = df_akas.join(df_basics, df_akas.titleId == df_basics.tconst, how='inner')
df_rez.show()

df_rez.select(df_rez.region, df_rez.isAdult) \
      .where(df_rez.isAdult == '1') \
      .show()

#df_rez1.show()

#sql_5 = df_akas.join(df_basics,'tconst','titleId').select('title','region').where(df_akas.isAdult == '1')
#sql_5.show()

#df_new = df_akas.union(df_basics)
#df_new.show()
