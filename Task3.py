from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()

df = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)

df.show()
#df.printSchema()
#df.describe('birthYear').show()
#df.select('birthYear').show(5)
#df.filter(1800<df.birthYear & df.birthYear<1900).show(5)
#df.select(df.primaryName, df.birthYear, df.birthYear.between(1800,1899)).show(5)
df.select(df.titleType, df.primaryTitle, df.originalTitle, df.runtimeMinutes) \
    .where(df.runtimeMinutes >120) \
    .show()

#df[df['primaryName'].str.contains('F')].show()
