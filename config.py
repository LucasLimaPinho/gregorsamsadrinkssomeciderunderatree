import os

BROKERS = 'localhost:9092,localhost:9093,localhost:9094'
ZOOKEEPER = 'localhost:2181'
GROUP_ID = 'raw-event-streaming-consumer'
TOPIC = 'samsa'
TOPIC_SAIDA = "strongbow"
CHECKPOINT_HDFS = 'hdfs://127.0.0.1:9000/app/checkpoint'
ID_SCHEMA_SAIDA = 1019528116