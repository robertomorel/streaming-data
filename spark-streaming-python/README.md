# Spark
## Documentação
Processamentos de streaming de dados em tempo real
- Tendências em redes sociais
- Estatísticas web
- Detecção de intrusão de sistemas

Extensão da API do Spark para processar dados em tempo real
- Recuperação rápida de falhas e atrasos
- Melhor balanceamento de carga e uso de recursos
- Combinação de streaming de dados com conjuntos de dados estáticos e consultas interativas
- Integração nativa com bibliotecas de processamento avançado (SQL, aprendizado de máquina, Grafos, etc.)

#### Spark Streaming APIs
- DStream (Discretized Stream) API
  - Stream “discretizado” = DStream = Sequência de RDDs
  - Baseada em RDDs
  - Não possui Engine SQL
  - Suporte a semântica de janelas temporais

  > Cada mensagem é uma entidade no streaming. Spark trabalha com streaming de dados usando a mesma abstração do RDD.

  > Stream “discretizado” = DStream = Sequência de RDDs

  > Entidades são agrupadas em batches. Cada batch é um RDD

  > Batches são formados com base em um intervalo de tempo (janelas)

- Structured Streaming API
  - Baseada em Dataframes
  - Possui Engine SQL
  - Suporte a semântica de janelas e eventos temporais  

## Instruções
### Criando Ambiente
- Baixando o Spark Hadoop

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

  `start-worker.sh spark://posgrad-vm:7077`

- Para finalizar o serviço do spark

  `stop-worker.sh`
   
  `stop-master.sh`  

> Acessar localhost:8080 no navegador e visualizar o Worker ativo:

### Testando o Spark Shell
- Rodar o comando: 

  `pyspark`

- Para sair

  `exit()`

### Netcat
- Netcat é uma ferramenta versátil para testes de rede o qual permite ler e escrever dados através das conexões, usando o protocolo TCP/IP. 
- Conexão TCP para enviar dados
- Simula um produtor
- Rodar o comando:

  `nc -lk 9999`
  
  > Subindo no localhost, porta 9999

### Executando a aplicação
- Rodar o comando:

  `/opt/spark/bin/spark-submit sparkStreaming.py localhost 9999`

  or

  `/opt/spark/bin/spark-submit sparkStreaming2.py localhost 9999`

  or

  `/opt/spark/bin/spark-submit sparkStreaming3.py localhost 9999`

  or

  `/opt/spark/bin/spark-submit sparkStreaming4.py localhost 9999`

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
[Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)