# Importa o Flask
from flask import Flask, render_template, Response, stream_with_context
from kafka import KafkaConsumer

# Cria uma instância do app no end-point (rota) "/"
app = Flask(__name__) 

@app.route("/") 
def hello(): # A rota "/" chama o método hello
  # Método retorna "hello world"
  #return "Hello World!" 
  return(render_template('index.html'))

# Para testar essa rota: 
## Criamos um producer no Kafka: bin/kafka-console-producer.sh --broker-list localhost:9092 --topic NOME_DO_TOPICO
## Rodamos esta aplicação
## Digita qualquer informação no produtor e verifica se está chegando no browser (que renderiza o consumo de streaming do flask)
@app.route('/topic/<topicname>')
def streamed_response(topicname): # A rota "/topic/<topicname>" chama o método streamed_response, que recebe um tópico por parâmetro
  stream = KafkaConsumer(topicname, bootstrap_servers='localhost:9092')

  def generate(): # Gerar um contexto (flask) para ser enviado como streming
    for message in stream:
      yield 'data:{0}\n\n'.format(message.value.decode())

  return Response(stream_with_context(generate()), mimetype="text/event-stream") 

# Rodando o python app.py
if __name__ == "__main__": 
  # Roda o app flask
  #app.run(port=5001)  
  app.run()