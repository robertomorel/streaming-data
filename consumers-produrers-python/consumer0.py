from kafka import KafkaConsumer

consumer = KafkaConsumer(
  'names', # Tópico
  bootstrap_servers=['localhost:9092'] # Config
)

for message in consumer:
  print(message)