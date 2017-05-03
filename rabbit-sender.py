# Rabbit MQ Testing in Python 3.5 - as a work queue
# Rabbit assumed to be running on localhost:5672

import pika

# Connecting to the localhost RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Creating a new Queue before we send messages to it
queue_name = 'test-queue-1'
channel.queue_declare(queue=queue_name)

channel.basic_publish(exchange='',
                      routing_key=queue_name,
                      body='Message 1')

print(" [x] Sent 'Message 1'")

connection.close()