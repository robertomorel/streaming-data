########################################################################
#Submetendo o job para o Spark .:. Rodando a aplicação
## /opt/spark/bin/spark-submit voos-dataframe.py
########################################################################
from pyspark.sql import SparkSession

if __name__ == "__main__":
  spark = SparkSession.builder.appName("App").getOrCreate()

  voos14 = spark.read.csv("file:///home/rober/workspace/streaming-de-dados/apache-spark-python/voos-2014.csv")
  voos15 = spark.read.csv("file:///home/rober/workspace/streaming-de-dados/apache-spark-python/voos-2015.csv")
  #voos14.show()
  #voos15.show()

  # Ler os dados incluindo o cabeçalho e esquema
  voos14 = spark.read.option("inferSchema", "true").option("header",True).csv("file:///home/rober/workspace/streaming-de-dados/apache-spark-python/voos-2014.csv")
  voos15 = spark.read.option("inferSchema", "true").option("header",True).csv("file:///home/rober/workspace/streaming-de-dados/apache-spark-python/voos-2015.csv")

  # Obter os 3 primeiros registros
  #voos15.take(3)
  # Ordenar e obter os 3 primeiros registros
  #voos15.sort("count").take(2)

  # Obter o maior número de voos entre dois países
  #from pyspark.sql.functions import max 
  #voos15.select(max("count")).take(1)
  
  # Obter o voo mais frequente em 2015
  #from pyspark.sql.functions import desc
  #voos15.sort(desc("count")).take(1)
  
  # Obter os 5 voos mais frequentes em 2015
  #voos15.sort(desc("count")).limit(5).show()
  
  # Renomear uma coluna
  #voos15.sort(desc("count")).withColumnRenamed("DEST_COUNTRY_NAME", "destino").limit(5).show()

  # Obter a quantidade total de voos em 2015
  #from pyspark.sql.functions import sum
  #voos15.select(sum("count")).take(1)
  
  # Obter a média de voos diários em 2015
  #voos15.select(sum("count")/365).take(1)

  # União
  #voos_concat = voos14.union(voos15)
  
  # Interseção
  #voos_int = voos14.intersect(voos15)

  # Quais os 5 países destino que mais receberam voos em 2015?
  #from pyspark.sql.functions import desc 
  #voos15.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)", "total_destino").sort(desc("total_destino")).limit(5).take(5)
  ##########################################
  ## [
  ##   Row(DEST_COUNTRY_NAME='United States', total_destino=411352), 
  ##   Row(DEST_COUNTRY_NAME='Canada', total_destino=8399), 
  ##   Row(DEST_COUNTRY_NAME='Mexico', total_destino=7140), 
  ##   Row(DEST_COUNTRY_NAME='United Kingdom', total_destino=2025), 
  ##   Row(DEST_COUNTRY_NAME='Japan', total_destino=1548)
  ## ]

  # Quais os 5 países destino que mais receberam voos em 2015? (em SQL)
  #voos15.createOrReplaceTempView("voos15")
  #spark.sql("SELECT DEST_COUNTRY_NAME, sum(count) as total_destino FROM voos15 group by DEST_COUNTRY_NAME order by sum(count) desc limit 5").show()
  ###################################
  #+-----------------+-------------+#
  #|DEST_COUNTRY_NAME|total_destino|#
  #+-----------------+-------------+#
  #|    United States|       411352|#
  #|           Canada|         8399|#
  #|           Mexico|         7140|#
  #|   United Kingdom|         2025|#
  #|            Japan|         1548|#
  #+-----------------+-------------+#
  ###################################

  # Qual a diferença no número total de voos entre 2014 e 2015?
  #voosTotal2015 = voos15.select(sum("count")).withColumnRenamed("sum(count)", "sum").take(1)[0]["sum"]
  #voosTotal2014 = voos14.select(sum("count")).withColumnRenamed("sum(count)", "sum").take(1)[0]["sum"]
  #dif = voosTotal2015-voosTotal2014
  #print(dif)

  # Qual a quantidade de voos total em 2014 entre os destinos "Bolivia" e "United States"?
  #total = voos14.filter((voos15["DEST_COUNTRY_NAME"] == "Bolivia") & (voos15["ORIGIN_COUNTRY_NAME"] == "United States") | (voos15["ORIGIN_COUNTRY_NAME"] == "Bolivia") & (voos15["DEST_COUNTRY_NAME"] == "United States")) \
  #  .select(sum("count")).withColumnRenamed("sum(count)", "sum") \
  #    .take(1)[0]["sum"]
  #print(total)    

  # Qual a quantidade de voos total em 2014 e em 2015 entre os destinos "Germany" e "United States"?
  #voos_union = voos14.union(voos15)
  #total = voos_union.filter((voos_union["DEST_COUNTRY_NAME"] == "Germany") & (voos_union["ORIGIN_COUNTRY_NAME"] == "United States") | (voos_union["ORIGIN_COUNTRY_NAME"] == "Germany") & (voos_union["DEST_COUNTRY_NAME"] == "United States")) \
  #  .select(sum("count")).withColumnRenamed("sum(count)", "sum") \
  #    .take(1)[0]["sum"]
  #print(total)

  # Qual a média de voos dentro dos EUA por dia considerando os anos de 2014 e 2015?
  #voos_union = voos14.union(voos15)
  #media = voos_union.select(sum("count")/365).take(1)[0]["(sum(count) / 365)"]
  #print(media)

  # Saber se existe algum registro de voos entre Brasil e EUA e em caso positivo, qual a quantidade no ano de 2014?
  #from pyspark.sql.functions import sum
  #voosBrEUA = voos14.filter((voos14["DEST_COUNTRY_NAME"] == "Brazil") & (voos14["ORIGIN_COUNTRY_NAME"] == "United States") | (voos14["ORIGIN_COUNTRY_NAME"] == "Brazil") & (voos14["DEST_COUNTRY_NAME"] == "United States")) \
  #  .select(sum("count")).withColumnRenamed("sum(count)", "sum") \
  #    .take(1)[0]["sum"]
  #if voosBrEUA > 0:
  #  print(voosBrEUA)  

  # Sumarizar os dados de 2014 e 2015 em um só Dataframe (Certifique-se de que não contenha registros repetidos).
  from pyspark.sql.functions import sum
  voos_union = voos14.union(voos15)
  voos_union.groupBy("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").sum("count").show()  

  spark.stop()