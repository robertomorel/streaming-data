from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
  spark = SparkSession \
    .builder \
    .appName("wordCountStructured") \
    .getOrCreate()

  # Lendo do Netcat (TCP)
  lines_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "9999") \
    .load()
    
  #lines_df.printSchema() # pode comentar essa linha depois  

  #explode: transforma um array de palavras em linhas (como flatmap)
  #expr: como explode e split são expressões, necessitamos usar 'expr'  
  # Justa todas as palavras das linhas com o separador ' '
  words_df = lines_df.select(expr("explode(split(value,' ')) as word"))

  # Contar a quantidade de palavras (alias 'word') 
  # counts_df = words_df.groupBy("word").count()

  counts_df = words_df.where("word = 'ERROR'").groupBy("word").count()

  # Escreve stream
  # Podemos salvar em qualquer banco de dados
  word_count_query = counts_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

  word_count_query.awaitTermination() 