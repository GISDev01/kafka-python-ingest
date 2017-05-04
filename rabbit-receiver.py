# Rabbit MQ Testing in Python 3.5 - as a work queue
# Rabbit assumed to be running on localhost:5672

import time

import pika

# Connecting to the localhost RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

queue_name = 'test-queue-d1'

# Creating a new Queue before we send messages to it
# Can declare this as many times as we want, it won't re-create or change an already create queue
channel.queue_declare(queue=queue_name, durable=True)


def callback(ch, method, properties, body):
    print("Received message body: {}".format(body))
    sleep_seconds_calc = body.count(b'.')
    time.sleep(sleep_seconds_calc)
    print('Done')
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Basic QOS which will only send messages to consumers after they have ack'ed their previous message. This keeps the load even throughout the consumers.
channel.basic_qos(prefetch_count=1)

channel.basic_consume(callback,
                      queue=queue_name)

print('Waiting for messages. To exit, use CTRL+C')
channel.start_consuming()
