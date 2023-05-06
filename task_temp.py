from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
simpleData = [("James",34),("Ann",34),
    ("Michael",33),("Scott",53),
    ("Robert",37),("Chad",27)
  ]

columns = ["firstname","age",]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.show()
#Output to console
#+---------+---+
#|firstname|age|
#+---------+---+
#|    James| 34|
#|      Ann| 34|
#|  Michael| 33|
#|    Scott| 53|
#+---------+---+

print(df.take(2))
#[Row(firstname='James', age=34), Row(firstname='Ann', age=34)]

print(df.tail(2))
#[Row(firstname='Robert', age=37), Row(firstname='Chad', age=27)]

print(df.first())
print(df.head())
#[Row(firstname='James', age=34)]

print(df.collect())
#[Row(firstname='James', age=34), Row(firstname='Ann', age=34), Row(firstname='Michael', age=33), Row(firstname='Scott', age=53), Row(firstname='Robert', age=37), Row(firstname='Chad', age=27)]

df.limit(3).show()

df = spark.read.option("delimiter", "\t").csv('F:/2023_Python/imdb-spark-project/imdb-data/name.basics.tsv.gz', header=True)
df.show()