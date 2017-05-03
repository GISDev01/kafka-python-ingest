import logging
import threading
import time
import json

import yaml
from kafka import KafkaProducer

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


class Producer1(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'),
                                 bootstrap_servers='localhost:9092')

        producer.send('topic3', {'Producer1': 'value1'})
        print('Sent Prod1 Message')
        time.sleep(3)
        self.run()


class Producer2(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'),
                                 bootstrap_servers='localhost:9092')

        producer.send('topic3', {'Producer2': 'value2'})
        print('Sent Prod2 Message')
        time.sleep(2)
        self.run()



def main():
    threads = [
        Producer1(),
        Producer2()
    ]

    for t in threads:
        t.start()
        logger.info('Thread started')

    logger.info('Sleeping for 100 seconds')
    time.sleep(100)


if __name__ == "__main__":
    main()
