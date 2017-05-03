# Rabbit MQ Testing in Python 3.5 - as a work queue
# Rabbit assumed to be running on localhost:5672

import pika
import time

# Connecting to the localhost RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

queue_name = 'test-queue-2'

# Creating a new Queue before we send messages to it
# Can declare this as many times as we want, it won't re-create or change an already create queue
channel.queue_declare(queue=queue_name)


def callback(ch, method, properties, body):
    print("Received message body: {}".format(body))
    sleep_seconds_calc = body.count(b'.')
    time.sleep(sleep_seconds_calc)
    print('Done')
    ch.basic_ack(delivery_tag = method.delivery_tag)


channel.basic_consume(callback,
                      queue=queue_name)


print('Waiting for messages. To exit, use CTRL+C')
channel.start_consuming()
