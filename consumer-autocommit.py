'''
In Apache Kafka, auto-commit records offsets by a preset interval.
This is often a useful for low velcoity applications where duplication is not important
In this example, the commit time is set to 1 second (1000 milliseconds)
'''
from kafka import KafkaConsumer
import json
import csv

# Kafka consumer configuration
topic = "twitter-sentiments"
brokers = "localhost:9092"
group_id = "0"


# Create the Kafka consumer
consumer = KafkaConsumer(topic,
                         bootstrap_servers=brokers,
                         group_id=group_id,
                         auto_offset_reset='latest',	#where to start reading, options are "earliest", "latest" and "none"
                         enable_auto_commit=True,
                         auto_commit_interval_ms=1000)

data_list = []
# Start consuming the data from the topic
for message in consumer:
    print(message.value)

# df = pd.read_json(data_list)
# df.to_csv('/home/keyur200494184/Tweets/Tweets.csv')
