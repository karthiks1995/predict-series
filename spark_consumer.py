import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-0-8-assembly_2.11-2.4.7.jar pyspark-shell'
os.environ['PYSPARK_PYTHON'] = 'python3.7'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3.7'
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import json

topic = 'electric'
broker = 'localhost:9092'

if __name__ == "__main__":
    sc = SparkContext(appName="PredictSeries")
    ssc = StreamingContext(sc, 2) # 2 second window
    # broker, topic = sys.argv[1:]
    # kafkaStream = KafkaUtils.createStream(ssc, broker, 'spark-streaming', {topic:1})
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": broker})
    # parsed = kafkaStream.map(lambda m: json.loads(m[1].decode('ascii')))
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    parsed.pprint()
    # parsed.count().map(lambda x: 'Tweets in this batch: %s' % x).pprint()
    # authors_dstream = parsed.map(lambda tweet: tweet['key'])
    # author_counts = authors_dstream.countByValue()
    # author_counts.pprint()
    # lines = kvs.map(lambda x: x[1])
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #               .map(lambda word: (word, 1)) \
    #               .reduceByKey(lambda a, b: a+b)
    # counts.pprint()
    ssc.start()
    ssc.awaitTermination()