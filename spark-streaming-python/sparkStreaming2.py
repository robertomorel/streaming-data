import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="StreamingErrorCount")
	ssc = StreamingContext(sc, 2) # 2 segundos de intervalo de tempo

	ssc.checkpoint("file:///home/rober/workspace/streaming-de-dados/spark-streaming-python/checkpoint") # tolerancia a falhas
	
	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta

	def contarPalavras(newValues, lastSum):
		if lastSum is None :
			lastSum = 0
		return sum(newValues, lastSum)

	#word_counts = lines.flatMap(lambda line: line.split(" "))\
	#		.filter(lambda word:"BOLBONARO" in word)\
	#		.map(lambda word : (word, 1))\
	#		.updateStateByKey(contarPalavras)

	#Sumarizar não mais a palavra e quantas vezes ela aparece, mas sim, a quantidade de palavras que existem com um determinado número de letras.
	#Exemplo:
	#Entrada: pao casa show cao loja disco
	#Saída esperada:
	#	3: 2 // (2 palavras com 3 letras)
	#	4: 3 // (3 palavras com 4 letras)
	#	5: 1 // (1 palavra com 5 letras)
	word_counts = lines.flatMap(lambda line: line.split(" "))\
			.map(lambda word : (len(word), 1))\
			.updateStateByKey(contarPalavras)

	word_counts.pprint() # imprime para cada intervalo. nao sao necessarios loops

	ssc.start() # inicia a escuta pelos dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
