from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()

print('title.akas')
df_akas = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.akas.tsv.gz', header=True)
df_akas.show(1)
print('title.basics')
df_basics = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)
df_basics.show(1)

df_rez_union = df_akas.join(df_basics, df_akas.titleId == df_basics.tconst, how='inner')
df_rez_union.show()

#Very long! Not effective!
Query_5 = df_rez_union.select(df_rez_union.region, df_rez_union.isAdult) \
      .where(df_rez_union.isAdult == '1') \
      .show()

