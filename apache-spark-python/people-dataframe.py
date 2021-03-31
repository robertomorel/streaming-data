########################################################################
#Submetendo o job para o Spark .:. Rodando a aplicação
## /opt/spark/bin/spark-submit people-dataframe.py
########################################################################
from pyspark.sql import SparkSession

if __name__ == "__main__":
  spark = SparkSession.builder.appName("App").getOrCreate()
  df = spark.read.csv("file:///home/rober/workspace/streaming-de-dados/apache-spark-python/people.json")

  df.show()
  df.printSchema()
  df.select(df.name).show()
  df.select(df['name'], df['age'] + 1).show()
  df.filter(df['age'] > 21).show()
  df.groupBy("age").count().show()
  df.orderBy("age", ascending = False).take(2)
  df.createOrReplaceTempView("pessoas")
  spark.sql("SELECT * FROM pessoas").show()

  spark.stop()