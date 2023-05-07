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

windowSpec = Window.orderBy('averageRating').partitionBy('genre')

New_table_join = df_ratings.join(df_basics,'tconst').select('originalTitle','averageRating','numVotes','startYear') \
    .withColumn('genre',f.col('genres')) \
    .filter(f.col('startYear') != '\\N').limit(10)
Sql_8  = New_table_join.withColumn('popular',f.mean(f.col('averageRating')).over(windowSpec))
Sql_8.show()
