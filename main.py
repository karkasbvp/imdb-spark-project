from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Karkasbvp').getOrCreate()
print('Session Started')
print('Code Executed Successfully')

def main():

    df = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/title.akas.tsv.gz', header=True)
    df.show(5)
    # novies_df = spark_session.csv(path)
    # novies_df.show()

if __name__ == "__main__":
    main()