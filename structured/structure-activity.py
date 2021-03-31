from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from time import sleep
from pyspark.sql.functions import expr

if __name__ == "__main__":
  spark = SparkSession \
    .builder \
    .appName("wordCountStructured") \
    .getOrCreate()

  # Evitando a criação de muitas partições 
  spark.conf.set("spark.sql.shuffle.partitions", 5)  

  static = spark.read.json("/home/rober/workspace/streaming-de-dados/files/activity-data/")
  static.show()
  dataSchema = static.schema

  ##########################################
  ### Etapa 1: leitura dos dados ###########
  ##########################################
  ### TODO: Baixar arquivo: wget www.lia.ufc.br/~timbo/streaming/activity-data.zip

  # Ler o json de maneira lenta: option("maxFilesPerTrigger", 1)
  # maxFilesPerTrigger: controlar a velocidade com que o Spark irá ler todos os arquivos da pasta (não muito usual na prática).
  streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1).json("/home/rober/workspace/streaming-de-dados/files/activity-data")  

  ##########################################
  ### Etapa 2: agrupamento #################
  ##########################################
  # Manipulando o streaming
  # gt: que atividade o usuário estava fazendo naquele instante
  #activityCounts = streaming.where("regra...").groupBy("gt").count()
  activityCounts = streaming.groupBy("gt").count()
  
  ##########################################
  ### Etapa 3: escrita do dado #############
  ##########################################
  # Criando view "activity_counts" e iniciando procedimento...
  # memory: irá guardar todo o dado em memória (por simplicidade, neste exemplo)
  # complete: reescreve todas as chaves e suas contagens a cada trigger
  activityQuery = activityCounts.writeStream.queryName("activity_counts").format("memory").outputMode("complete").start()

  ##########################################
  ### Etapa 4: imprimindo dados ############
  ##########################################
  for x in range(10):
    spark.sql("SELECT * FROM activity_counts").show()
    sleep(5)  

  # Cria coluna nova no DF: withColumn. Terá tudo onde "gt like '%stairs%'" 
  # Filtra por "stairs" e gt não nulo 
  # append: novos resultados são adicionados à resposta
  simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))\
    .where("stairs")\
    .where("gt is not null")\
    .select("gt", "model", "arrival_time", "creation_time")\
    .writeStream\
    .queryName("simple_transform")\
    .format("memory")\
    .outputMode("append")\
    .start()    

  activityQuery.awaitTermination() # usar só via script  