from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()

df = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)
df.show(5)

Query_3 = df.select(df.titleType, df.primaryTitle, df.originalTitle, df.runtimeMinutes) \
    .where(df.runtimeMinutes >120)
Query_3.show(5)

