import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="StreamingErrorCount")
	ssc = StreamingContext(sc, 2) # 2 segundos de intervalo de tempo

	ssc.checkpoint("file:///home/rober/workspace/streaming-de-dados/spark-streaming-python/checkpoint") # tolerancia a falhas
	
	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta

	# Aplicar função de agregação por janela (reduce por janela)
	# Somar os números nos últimos 10s (lambda x, y: int(x) + int(y),)
	# Depois que passar o tempo da janela, outra função é chamada, para voltar os dados (lambda x, y: int(x) - int(y),)
	# 2 é o slide interval
	soma = lines.reduceByWindow(
                lambda x, y: int(x) + int(y),
                lambda x, y: int(x)/lines.count(),
                16,
                2
        )	


	#linesQde = lines.count()

	soma.pprint() # imprime para cada intervalo. nao sao necessarios loops

	ssc.start() # inicia a escuta pelos dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
