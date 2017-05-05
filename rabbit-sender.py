# Rabbit MQ Testing in Python 3.5 - as a work queue
# Rabbit assumed to be running on localhost:5672

import json
import sys

import pika

test_msg = ' '.join(sys.argv[1:])

# Connecting to the localhost RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Creating a new Queue before we send messages to it
rabbit_queue_name = 'ids_queue'

channel.queue_declare(queue=rabbit_queue_name, durable=True)

# can list this queue in a shell with: sudo rabbitmqctl list_queues


for i in range(100):
    ids_to_send = [i, i + 1000, i + 10000]
    sample_msg = {'ids': ids_to_send}
    channel.basic_publish(exchange='',
                          routing_key=rabbit_queue_name,
                          body=json.dumps(sample_msg),
                          properties=pika.BasicProperties(
                              # Make this message persistent
                              delivery_mode=2,
                          ))

print("Sent Message: {}".format(sample_msg))

connection.close()
