# Spark

## Structured Streaming API
- Structured Streaming API
  - Baseada em Dataframes
  - Possui Engine SQL
  - Suporte a semântica de janelas e eventos temporais  

### Netcat
- Netcat é uma ferramenta versátil para testes de rede o qual permite ler e escrever dados através das conexões, usando o protocolo TCP/IP. 
- Conexão TCP para enviar dados
- Simula um produtor
- Rodar o comando:

  `nc -lk 9999`

## Executando a aplicação
- Rodar Netcat
- Rodar o programa

  `/opt/spark/bin/spark-submit wordCountStructured.py`  

  ou

  `/opt/spark/bin/spark-submit structure-activity.py`

## Documentação
[RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)  
[Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)