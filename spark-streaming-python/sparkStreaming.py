import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

	# Importando o contexto, pois estamos lidando com RDD
	sc = SparkContext(master="local[2]", appName="StreamingErrorCount")

	# SSC - Streaming Spark Context
	# 2 segundos de intervalo de tempo para ler o contexto
	ssc = StreamingContext(sc, 2) 

	# Tolerancia a falhas (boa prática)
	ssc.checkpoint("file:///home/rober/workspace/streaming-de-dados/spark-streaming-python/checkpoint") 
	
	# Lines eh uma sequencia de RDDs
	# sys.argv[1]: host
	# sys.argv[2]: porta
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta

	# Contagem de WARN
	counts = lines.flatMap(lambda line: line.split(" "))\
			.filter(lambda word:"WARN" in word)\
			.map(lambda word: (word, 1))\
			.reduceByKey(lambda a, b: a+b)

	#1. Alterar o intervalo de tempo para 10 segundos
	## ssc = StreamingContext(sc, 10)
	#2. Alterar a palavra buscada para "ERROR"
	## .filter(lambda word:"ERROR" in word)\
  #3. Filtrar apenas por palavras que começam com a letra A (maiúsculo)
	## .filter(lambda word:word.startswith("A"))   
  #4. Filtrar apenas por palavras que terminam com um número qualquer. ex. qwe4, des11, cvb0		
  ## .filter(lambda word:word.endswith("1"))\

	# Imprime para cada intervalo. nao sao necessarios loops
	counts.pprint() 

	# Inicia a escuta pelos dados de streaming
	ssc.start() 
	# Aplicacao espera terminar os dados de transmissao
	ssc.awaitTermination()  
