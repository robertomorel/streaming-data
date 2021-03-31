import time

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# Envia no tópico 'names' a mensagem 'Felipe'
producer.send('names', 'Felipe'.encode('utf-8'))
producer.send('names2', 'Carlos'.encode('utf-8'))
producer.send('names', 'Eduarda'.encode('utf-8'))

time.sleep(20)

from kafka import KafkaProducer

time.sleep(20)