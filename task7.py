from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as f

spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()
print('title.ratings')
df_ratings = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.ratings.tsv.gz', header=True)
df_ratings.show(1)
print('title.basics')
df_basics = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.basics.tsv.gz', header=True)
df_basics.show(1)

windowSpec = Window.orderBy('averageRating').partitionBy('decades')

New_table_join = df_ratings.join(df_basics,'tconst').select('originalTitle','averageRating','numVotes','startYear') \
    .withColumn('decades',f.floor(df_basics.startYear / 10)).orderBy('startYear',ascending=False) \
    .filter(f.col('startYear') != '\\N').limit(10)

Query_7  = New_table_join.withColumn('popular',f.mean(f.col('averageRating')).over(windowSpec))
Query_7.show()
