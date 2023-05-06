from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
print('Session Started')
print('Code Executed Successfully')

def main():
   df = spark.read.csv('F:/2023_Python/imdb-spark-project/imdb-data/name.basics.tsv.gz', header=True)
 # df = spark.read.csv('F:/2023_Python/imdb-spark-project/imdb-data/title.akas.tsv.gz', header=True)
   df.show()

    # novies_df = spark_session.csv(path)
    # novies_df.show()

if __name__ == "__main__":
    main()