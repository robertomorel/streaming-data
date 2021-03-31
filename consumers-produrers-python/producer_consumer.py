import json
import random
import threading
import time

from kafka import KafkaConsumer, KafkaProducer

# threading.Thread -> este argumento significa que esta classe irá rodar em uma thread
class Producer(threading.Thread):

    def run(self):

        # Serializando a mensagem com a função "lambda"
        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
            data = {}
            id_ = random.randint(0, 1000)

            if data.__contains__(id(id_)):
                message = data.get(id_)
            else:
                streaming = {'idade': random.randint(10, 50), 'altura': random.randint(100, 200),
                            'peso': random.randint(30, 100)}
                message = [id_, streaming]
                data[id_] = message

            producer.send('cadastro', message)
            #time.sleep(random.randint(0, 5))
            time.sleep(5)

# threading.Thread -> este argumento significa que esta classe irá rodar em uma thread
class Consumer(threading.Thread):

    def run(self):
        # auto_offset_reset -> Auto reseta os offsets de acordo com as últimas mensagens
        stream = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='latest')
        
        # O consumidor está se inscrevendo no tópico 'topic'
        stream.subscribe(['cadastro'])
        #stream.subscribe(['topic'])

        #altura = ...
        #peso = ...
        #imc = peso/altura**2
        
        # Para cada mensagem (tupla), imprimir
        for tuple in stream:
            # Deserializando a mensagem com a função "json.loads"
            obj = json.loads(tuple.value)[1]
            imc = obj['peso'] / (obj['altura']/100)**2
            if imc > 35:
                print(imc)
                print(obj)

# Função main
if __name__ == '__main__':
    # Instânciando threads, para executar em paralelo
    threads = [
        Producer(),
        # Producer('topico'),
        # Producer(),
        # Producer(),
        Consumer()
    ]

    # Percorre cada thread iniciando as classes
    for t in threads:
        t.start()
