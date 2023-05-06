from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()

df = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/name.basics.tsv.gz', header=True)

df.show()
df.select(df.primaryName, df.birthYear) \
    .where(df.birthYear.between(1800,1899)) \
    .show()


