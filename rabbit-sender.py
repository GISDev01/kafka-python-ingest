# Rabbit MQ Testing in Python 3.5 - as a work queue
# Rabbit assumed to be running on localhost:5672

import pika
import sys

message = ' '.join(sys.argv[1:]) or "Hardcoded Test Msg1"

# Connecting to the localhost RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Creating a new Queue before we send messages to it
queue_name = 'test-queue-2'
channel.queue_declare(queue=queue_name)

# can list this queue in a shell with: sudo rabbitmqctl list_queues


channel.basic_publish(exchange='',
                      routing_key=queue_name,
                      body=message,
                      properties=pika.BasicProperties(
                          # Make this message persistent
                          delivery_mode=2,
                      ))

print("Sent Message: {}".format(message))


connection.close()
