## Instruções
### Subindo Servidores
- Inicializando o Zookeeper

  `bin/zookeeper-server-start.sh config/zookeeper.properties`

- Inicializando o Servidor Kafka

  `bin/kafka-server-start.sh config/server.properties`

### Rodando a Aplicação
- Executando o produtor

  `python3 producer1.py`

- Executando a aplicação

  `python3 app.py`

### Para Testes com o Produtor
- Crie o ambiente à partir do README na pasta <i>'../consumers-producers-python/'</i>

- Rode o comando:

  `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic NOME_DO_TOPICO`    
