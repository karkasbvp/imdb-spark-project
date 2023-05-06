from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
spark = SparkSession.builder.appName('SparkByExamples').getOrCreate()

#spark = SparkSession.builder.appName('abc').getOrCreate()
print('Session Started')
print('Code Executed Successfully')

def main():
   # spark_session = SparkSession.builder.master("local").appName("IMDB analysis app").getOrCreate()

 # df = spark_session.read.csv('F:/2023_Python/imdb-spark-project/imdb-data/name.basics.tsv.gz')
    df = spark.read.option(sep=r'\t').csv('F:/2023_Python/imdb-spark-project/imdb-data/name.basics.tsv.gz', header=True)
    df.show()

 # df = spark.read.csv('F:/2023_Python/imdb-spark-project/imdb-data/title.akas.tsv.gz', header=True)
 #  df.sql_ctx.show()
 #  df.limit(3).show()
#   df.first()
#   df.printSchema()

    # novies_df = spark_session.csv(path)
    # novies_df.show()

if __name__ == "__main__":
    main()