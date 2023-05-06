from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()

df_basics = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)
df_basics.show()

df_principals = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.principals.tsv.gz', header=True)
df_principals.show()

sql_4 = df_principals.join(df_basics,'tconst').select('originalTitle','characters','category').where(df_principals.category == 'actor')
sql_4.show()


