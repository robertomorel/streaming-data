########################################################################
#Submetendo o job para o Spark .:. Rodando a aplicação
## /opt/spark/bin/spark-submit alunos-demo.py
########################################################################
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("alunos-demo")
sc = SparkContext(conf=conf)

alunos = sc.textFile("file:///home/rober/workspace/streaming-de-dados/files/alunos.csv")

# Imprime as duas primeiras linhas
alunos.take(2)
# Filtra alunos que tenham 'J' no nome
alunosComJ = alunos.filter(lambda row: 'J' in row)
# Imprime as duas primeiras linhas
alunosComJ.take(2)
# Gera array [1,2,3,4]
nums = sc.parallelize([1, 2, 3, 4])
numsMaisUm = nums.map(lambda x: x+1)
nums2 = sc.parallelize([4, 5, 6])
nums.union(nums2).take(10)
nums.intersection(nums2).take(10) 

alunos.saveAsTextFile("file:///home/rober/workspace/streaming-de-dados/files/output/alunos")
nums.saveAsTextFile("file:///home/rober/workspace/streaming-de-dados/files/output/numeros")