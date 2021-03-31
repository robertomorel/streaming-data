import json
import random
import threading
import time
from faker import Faker

from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from json import loads

fake = Faker()

class Producer(threading.Thread):

    def run(self):

        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
            data = {}
            id_ = random.randint(0, 1000)

            if data.__contains__(id(id_)):
                message = data.get(id_)
            else:
                streaming = {'nome': fake.name(), 'idade': random.randint(10, 50), 'altura': random.randint(100, 200),
                            'peso': random.randint(30, 100)}
                message = streaming
                data[id_] = message

            producer.send('users', message)
            time.sleep(random.randint(0, 5))

class Consumer(threading.Thread):

    # value_deserializer: deserializa o utf-8 do json
    def run(self):
        stream = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='latest', value_deserializer = lambda x: loads(x.decode('utf-8')))
        client = MongoClient('localhost:27017')
        collection = client.users.pessoas  # client.banco_de_dados.colecao

        stream.subscribe(['users'])
        for tuple in stream:
            msg = tuple.value
            # print(msg)
            if msg['nome'][0] == 'J':
                collection.insert_one(msg)
                print('{} added to {}'.format(msg, collection))

if __name__ == '__main__':
    threads = [
        Producer(),
        # Producer(),
        # Producer(),
        # Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()
