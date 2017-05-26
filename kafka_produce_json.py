from kafka import KafkaProducer
import json

with open('raw_data/tweets.txt') as f:
	json_data = f.read().splitlines()

producer = KafkaProducer(bootstrap_servers=[''])
i = 0
for l in json_data:
	print i
	producer.send('mock_twitter_stream', l)
	i += 1
producer.flush()
