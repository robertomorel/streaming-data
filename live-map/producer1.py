from kafka import KafkaProducer
from datetime import datetime

import json
import uuid
import time

#LER AS COORDENADAS GEOJSON
input_file = open('./data/bus1.json')
json_array = json.load(input_file)

coordinates = json_array['features'][0]['geometry']['coordinates']

#GERAR ID UNICO
def generate_uuid():
  return uuid.uuid4()

#KAFKA PRODUCER
producer = KafkaProducer(bootstrap_servers='localhost:9092')

#CONSTRUIR A MENSAGEM E ENVIAR VIA KAFKA
data = {}
data['busline'] = '00001'

def generate_checkpoint(coordinates):
  i = 0
  #PERCORRE CADA COORDENADA
  while i < len(coordinates):
    #ATUALIZA OBJETO DE DADOS
    data['key'] = data['busline'] + '_' + str(generate_uuid()) #ID
    data['timestamp'] = str(datetime.utcnow())
    data['latitude'] = coordinates[i][1]
    data['longitude'] = coordinates[i][0]

    message = json.dumps(data)
    print(message)
    
    producer.send('busao', message.encode('ascii'))
    # producer.produce(message.encode('ascii'))
    
    time.sleep(1)
    
    #if bus reaches last coordinate, start from beginning
    if i == len(coordinates)-1:
      i = 0
    else:
      i += 1

generate_checkpoint(coordinates)

#Criar um Consumer na pasta do Kafka para testar esse Producer
## bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic busao
#Lembrar de iniciar o Zookeeper e o server Kafka
## bin/zookeeper-server-start.sh config/zookeeper.properties
## bin/kafka-server-start.sh config/server.properties