## Instruções
### Instalando algumas libs
- Instalar o curl

  `sudo apt install curl`
  
- Instalar o VSCode

  `sudo snap install --classic code`
  
- Instalar o Python

  `sudo apt install python3-pip`
  
- Instalar o Java

  `sudo apt install default-jdk`

### Apache Kafka
- Download: curl

  `wget http://ftp.unicamp.br/pub/apache/kafka/2.7.0/kafka_2.12-2.7.0.tgz -o ~/Downloads/kafka.tgz`

- Criar um diretório chamado kafka e entrar nele:

  `mkdir kafka`

  `cd kafka`

- Extrair os arquivos que estão na pasta download para o diretório criado:

  `tar -xvzf ~/Downloads/kafka.tgz --strip 1`
 
### Subindo Servidores
- Inicializando o Zookeeper

  `bin/zookeeper-server-start.sh config/zookeeper.properties`

- Inicializando o Servidor Kafka

  `bin/kafka-server-start.sh config/server.properties`
