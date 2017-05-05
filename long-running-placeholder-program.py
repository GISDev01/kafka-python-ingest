# Long-running program that uses an input which is received from a RabbitMQ Message

import time

import pika
import json

import logging

LOG_FORMAT = '%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

rabbit_hostname = 'localhost'
rabbit_queue_name = 'ids_queue'

# Connecting to the RabbitMQ server
rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(rabbit_hostname))

# Open a blocking channel to consume messages on
rabbit_channel = rabbit_conn.channel()

# Durable allows the queue to survive reboots of the Rabbit box
rabbit_channel.queue_declare(queue=rabbit_queue_name, durable=True)


# This fires as a callback every time we receive a new message on the queue
def rabbit_msg_receiver(ch, method, properties, body):
    logger.info("Received message body: {}".format(body))
    # Need to decode the bytes body into a string, so we can load into a dict from a raw json string
    msg_dict = json.loads(body.decode('utf-8'))
    ids_to_send_to_program = msg_dict['ids']
    logger.info("Sending to long-running process: {}".format(ids_to_send_to_program))

    long_running_processing(ids_to_send_to_program)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def long_running_processing(ids_to_process):
    sleep_seconds_calc = 1000
    logger.info('IDs sent to long running process: {} '.format(ids_to_process))

    #Block the long-running function for a really long time and make sure no messages are ingested while this function is running
    time.sleep(sleep_seconds_calc)


# Basic QOS which will only send messages to consumers after they have ack'ed their previous message. This keeps the load even throughout the consumers.
rabbit_channel.basic_qos(prefetch_count=1)

rabbit_channel.basic_consume(rabbit_msg_receiver,
                             queue=rabbit_queue_name)

print('Waiting for messages indefinitely on Queue: {} -  To exit, use CTRL+C'.format(rabbit_queue_name))

rabbit_channel.start_consuming()
