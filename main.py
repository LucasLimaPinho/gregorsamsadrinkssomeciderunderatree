# coding=utf-8
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import sys
import io
import json
import os
import struct
import datetime as dt
from datetime import datetime
import config
import avro.schema
import urllib2
from avro.io import BinaryDecoder, DatumReader, DatumWriter
from kafka import KafkaProducer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import time


SCHEMA_REST = ""
schemas_local = {}

schema_target = avro.schema.parse(
    open("data/target.avsc", "rb").read())

schema_source = avro.schema.parse(
    open("data/source.avsc", "rb").read())

avro_writer = DatumWriter(schema_target)

avro_writer2 = DatumWriter(schema_source)
avro_reader = DatumReader(schema_source)

keys = [
    "apiEntrypointName",
    "apiName",
    "apiVersion",
    "method",
    "responseCode",
    "apiApplicationName"]

keys2 = [
    "apiEntrypointName",
    "apiName",
    "apiVersion",
    "method",
    "responseCode",
    "apiApplicationName",
    "request_count"]

records = {

    "apiVersion": '/v2',
    "apiName": 'Gerente de Instalações Industriais',
    "apiApplicationName": 'Novo Front Personalizado',
    "apiEntrypointName": 'production-int',
    "method": 'OPTIONS',
    "responseCode": '200'
}


def get_schema(schema_id):
    if (not schema_id in schemas_local):

        uri = SCHEMA_REST + schema_id
        req = urllib2.Request(uri)
        url = urllib2.urlopen(req)

        try:

            sc = json.loads(url.read().decode())
            schemas_local[schema_id] = sc['schema']
            return sc['schema']
        except Exception as e:
            print("Error parsing JSON")
            print(e)


def request_counting(partitions):
    for partition in partitions:
        partition = partition[0] + ("|") + str(partition[1])
        yield partition


def build_row(value):
    try:

        epoch_kibana =value['timestamp']
        epoch_kibana_sem_3_digitos = epoch_kibana[:-3]
        timestamp_kibana = datetime.fromtimestamp(float(epoch_kibana_sem_3_digitos)).strftime("%Y-%m-%d %H:%M")
        tempo_str = str(timestamp_kibana) + ':00'
        date_time_string = time.strptime(tempo_str, '%Y-%m-%d %H:%M:%S')
        epoch_timestamp = str(time.mktime(date_time_string)) + '000'
        value['timestamp'] = epoch_timestamp

        if (
                value is not None
                and value["apiEntrypointName"] not in [None, "description"]
                # no null key
                and all([value.get(k) is not None for k in keys])
        ):

            # join values using pipe character
            return ('|'.join([value.get(k) for k in keys])).encode('utf-8')

        else:

            return

    except Exception:

        return


def send_message(rows):
    producer2 = KafkaProducer(bootstrap_servers=brokers, value_serializer=avro_encoder)

    for row in rows:
        columns = row.split("|")
        message = {k: columns[i] for i, k in enumerate(keys2)}
        message['extraInfo'] = {"version": "hub_agregacao_streaming_v1"}
        producer2.send(config.TOPIC_SAIDA, message)
        producer2.flush()


def avro_encoder(value):
    schema_id = config.ID_SCHEMA_SAIDA
    packed = struct.pack('>i', schema_id)
    bytes_writer = io.BytesIO()
    bytes_writer.write(b'\x00')
    bytes_writer.write(packed)
    avro_writer.write(value, avro.io.BinaryEncoder(bytes_writer))

    return bytes_writer.getvalue()


def avro_encoder2(value):
    schema_id = config.ID_SCHEMA_SAIDA
    packed = struct.pack('>i', schema_id)
    bytes_writer = io.BytesIO()
    bytes_writer.write(b'\x00')
    bytes_writer.write(packed)
    avro_writer2.write(value, avro.io.BinaryEncoder(bytes_writer))

    return bytes_writer.getvalue()


def avro_decoder(value):
    unpacked = struct.unpack('>i', value[1:5])[0]
    # schema_str = get_schema(str(unpacked))
    # if (schema_str != None):
    #    schema = avro.schema.parse(schema_str)
    bytes_reader = io.BytesIO(value[5:])
    decoder = BinaryDecoder(bytes_reader)
    # avro.reader = avro.io.DatumReader(schema)
    return avro_reader.read(decoder)


def initialization(partitions):
    for partition in partitions:
        partition = partition[1]
        partition = build_row(partition)
        partition_tuple = (partition, 1)
        yield partition_tuple


def produce_messages():
    producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=avro_encoder2)
    num_eventos = 5

    for i in range(num_eventos):
        message = records
        producer.send(config.TOPIC, message)
        producer.flush()


def create_context():
    sc = SparkContext(appName="TesteParalelizacao")
    sc.setLogLevel('ERROR')

    ssc = StreamingContext(sc, 2)

    kvs = KafkaUtils.createDirectStream(ssc, [config.TOPIC], {"metadata.broker.list": brokers},
                                        valueDecoder=avro_decoder)

    # Spark resolves around the concept of a RESILIENT DISTRIBUTED DATASET (RDD), which is fault-tolerant collection
    # of elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing
    # collection in your driver program or referencing a dataset in a external storage system, such as a shared filesystem
    # HDFS, HBase or any data source offering a HADOOP InputFormat.

    # RDD support two types of operations: transformations, which create a new dataset from an existing one, and actions,
    # which can return a value to the driver program after running a computation on the dataset. For example, map is a
    # transformation that passes each dataset element through a function that returns a new RDD representing the results.
    # On the other hand, reduce is an action that aggregates all the elements of the RDD using some function and returns
    # the final result to the driver program (although there is also a parallel reduceByKey that returns a distributed dataset.

    # To execute jobs, Spark breaks up processing of RDD operations into tasks, each of which is executed by an executor.
    # Prior to execution, Spark computes the tasks clouse. The closure is those variables and methods which must be visible
    # for the executor to perform its computations on the RDD. The clousure is serialized and sent to each executor.

    # reduceByKeyAndWindow

    kvs0 = kvs.mapPartitions(initialization)
    kvs1 = kvs0.filter(lambda row: row is not None)
    kvs2 = kvs1.reduceByKey(lambda x, y: x + y)
    kvs3 = kvs2.mapPartitions(request_counting)
    kvs4 = kvs3.foreachRDD(lambda rdd: rdd.foreachPartition(send_message))

    print("Mensagens Enviadas vindo do create context..")

    return ssc


if __name__ == '__main__':
    brokers = config.BROKERS
    zookeeper = config.ZOOKEEPER
    topic = config.TOPIC

    # Aqui devemos ter um diretório que seja "HDFS Compatible". Irá estourar exceção no momento de tentar escrever no
    # no checkpoint
    # TODO: tentar utilizar S3 Bucket provisoriamente para armazenamento de checkpoint - mas pode apresentar problemas
    # de inconsistência

    checkpoint = "./checkpoint"

    ssc = StreamingContext.getOrCreate(checkpoint, create_context)

    ssc.start()
    produce_messages()
    ssc.awaitTermination()
