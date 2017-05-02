import logging
import threading
import time

import yaml
from kafka import KafkaConsumer, KafkaProducer

with open("config.yml", 'r') as yaml_config_file:
    config = yaml.load(yaml_config_file)

LOG_FORMAT = '%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

db_hostname = config['database']['hostname']
db_port = config['database']['port']
db_name = config['database']['dbname']
db_user = config['database']['user']
db_pwd = config['database']['pwd']


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer("topic3", group_id="group1",
                                 bootstrap_servers='localhost:9092')
        for message in consumer:
            print(message)
            print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(message.timestamp / 1000.0)))


def main():
    threads = [
        Consumer()
    ]

    for t in threads:
        t.start()
        logger.info('Thread started')

    logger.info('Sleeping for 100 seconds')
    time.sleep(100)


if __name__ == "__main__":
    main()
