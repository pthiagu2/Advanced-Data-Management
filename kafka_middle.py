from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
consumer = KafkaConsumer('mock_twitter_stream')
#producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers=[''])

#producer.send('topic2', 'aa')

for msg in consumer:
	text = msg[6]
	data = json.loads(text)
	if "limit" in data:
		continue
	#print data
	producer.send('spark_input', msg[6])
producer.flush()

