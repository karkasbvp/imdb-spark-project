from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()

df = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.akas.tsv.gz', header=True)
df.show()
df.select(df.title, df.region, df.types, df.attributes) \
     .where(df.region == 'UA') \
     .show()
