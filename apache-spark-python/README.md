# Spark
## Documentação
“A fast and general engine for large-scale data processing”
- Manipulações, transformações e análises complexas
- Aprendizagem de máquina
- Mineração de dados
- Análise de grafos
- Streaming
- Linguagens: Python, Java, e Scala

## Instruções
### Criando Ambiente
- Baixando Spark Hadoop

  `wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz ~/Downloads/spark-hadoop.tgz`

- Extrair os arquivos

  `tar -xvzf spark-*`

- Mover os arquivos para o diretório ~ opt/spark

  `sudo mv spark-3.1.1-bin-hadoop2.7 /opt/spark`

### Variáveis de Ambiente
- Configurando variáveis

  `echo "export SPARK_HOME=/opt/spark" >> ~/.profile`
  
  `echo "export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin" >> ~/.profile`
  
  `echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile`

- Testando variáveis

  `source ~/.profile`

### Iniciando Spark
- Master

  `start-master.sh`

- Trabalhador

  `start-slave.sh spark://posgrad-vm:7077`

- Para finalizar o serviço do spark

  `stop-slave.sh`
  
  `stop-master.sh`  

> Acessar localhost:8080 no navegador e visualizar o Worker ativo:

### Testando o Spark Shell
- Rodar o comando: 

  `pyspark`

- Para sair

  `exit()`

## Comandos
### Exemplos de Comandos
```bash
nums = sc.parallelize([1, 2, 3, 4])
nums.count()
nums.collect()
```

### Transformações
Observar as sequencias de (chave,valor)

- Map

  `sc.parallelize([1, 2, 3, 4,5]).map(lambda x: x + 10).collect()`
  > Return: {11,12,13,14,15}
- Filter

  `sc.parallelize([1, 2, 3, 4, 5]).filter(lambda x: x == 3).collect()`
  > Return: 3

- Flat Map

  `sc.parallelize(["this is line 1", "this is the second line"]).flatMap(lambda line: line.split(" ")).collect()`
  > Return: ['this', 'is', 'line', '1', 'this', 'is', 'the', 'second', 'line']

- Union
  `sc.parallelize([1, 2, 3, 4, 5]).union(sc.parallelize([6, 7, 8, 9,10])).collect()`
  > Return: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

- Intersection
  
  `sc.parallelize([1, 2, 3, 4, 5]).intersection(sc.parallelize([4, 5, 6, 7, 8])).collect()`
  > Return: [4, 5]

- Distinct

  `sc.parallelize([(1,2), (1,2), (1,2), (3,4), (5,6), (7,8), (7,8)]).distinct().collect()`
  > Return: [(1, 2), (3, 4), (5, 6), (7, 8)] 

- Reduce By Key

  `sc.parallelize([(1,2), (1,2), (1,2), (3,4), (5,6), (7,8), (7,8)]).reduceByKey(lambda x, y: x + y).collect()`
  > Return: [(1, 6), (5, 6), (3, 4), (7, 16)]

- Sort By Key
  
  `sc.parallelize([(1,2), (1,2), (1,2), (3,4), (5,6), (7,8), (7,8)]).sortByKey().collect()`
  > Return: [(1, 2), (1, 2), (1, 2), (3, 4), (5, 6), (7, 8), (7, 8)]

- Join

  `sc.parallelize([(1,"um"), (2,"dois"), (3,"tres")]).join(sc.parallelize([(1,"one"), (2, "two"), (3, "three")])).collect()`
  > Return: [(1, ('um', 'one')), (2, ('dois', 'two')), (3, ('tres', 'three'))] 

### Ações  
- Reduce
  
  `sc.parallelize([1,2,3,4,5]).reduce(lambda x, y: x + y )`
  > Return: 15

- Collect

  `sc.parallelize([1,2,3,4,5]).collect()`
  > Return: [1, 2, 3, 4, 5]

- Count

  `sc.parallelize([1,2,3,4,5]).count()`
  > Return: 5

- First
  
  `sc.parallelize([1,2,3,4,5]).first()`
  > Return: 1

- Take

  `sc.parallelize([1,2,3,4,5]).take(2)`
  > Return: [1, 2]

- Count By Value

  `sc.parallelize([1,2,3,4,5]).countByValue(2)`
  > Return: --

- Top
  
  `sc.parallelize([1,2,3,4,5]).top(2)`
  > Return: [5, 4]

## Documentação
[RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)  
