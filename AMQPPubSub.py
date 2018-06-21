import sys
import os
import inspect
import pika
import threading


class AMQPPubSub:
    def __init__(self, params):
        
        self.url = params["url"]
        self.port = params["port"]
        self.timeout = params["timeout"]
        self.exchange = params["exchange"]
        
        try:
            #credentials = pika.PlainCredentials(params['username'], params['password'])
            #parameters = pika.ConnectionParameters(self.url, self.port, '/', credentials)
            parameters = pika.ConnectionParameters(self.url, self.port)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange,exchange_type='topic', durable=True)
            result = self.channel.queue_declare(exclusive=True)
            self.queue_name = result.method.queue
            self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key='#')
            print('Connected to  ', self.exchange)
        except:
            print('Could not connect to  ', self.exchange)

        if ("onMessage" in params):
            self.on_message = params["onMessage"]

    def publish(self, topic, payload):
        self.channel.basic_publish(exchange=self.exchange, routing_key=topic, body=payload)

    def connect(self):
        self.channel.basic_consume(self.on_message, queue=self.queue_name, no_ack=True)
        self.channel.start_consuming()
    def run(self):
        threading.Thread(target=self.connect).start()
