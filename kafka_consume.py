from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('spark_input')

i = 0
for msg in consumer:
	data = json.loads(msg[6])
	
	#print msg[0], msg[6]
	i +=1
print "Total tweets consumed from spark_input",i