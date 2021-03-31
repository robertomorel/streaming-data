########################################################################
#Submetendo o job para o Spark .:. Rodando a aplicação
## /opt/spark/bin/spark-submit wordcount.py
########################################################################
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("ContagemDePalavras")
sc = SparkContext(conf=conf)

text_file = sc.textFile("file:///home/rober/workspace/streaming-de-dados/apache-spark-python/shakespeare.txt")

counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("file:///home/rober/workspace/streaming-de-dados/files/output/words")


#1. ler aquivo: "Famoso Famoso Famoso é aquele que"
#2. flatmap: "Famoso" "Famoso" "Famoso" "é" "aquele" "que"
#3. map: ("Famoso", 1) ("Famoso", 1) ("Famoso", 1) ("é", 1) ("aquele", 1) ("que", 1)
#4. reduceByKey: ("Famoso", 3) ("é", 1) ("aquele", 1) ("que", 1)
