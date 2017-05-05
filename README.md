# py-kafka-and-rabbitmq
Some testing of using the python library over the usual Java library when dealing with Kafka streams and RabbitMQ Queues.

For Windows, get a local Kafka up and running quickly by running these 2 commands in 2 separate shells:
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
bin\windows\kafka-server-start.bat config\server.properties

For Windows, get RabbitMQ installed by following their instructions here: https://www.rabbitmq.com/download.html

For Mac, you can get RabbitMQ up and running quickly with homebrew: brew install rabbitmq
After the brew runs, you can fire up RabbitMQ on your Mac with the command: rabbitmq-server