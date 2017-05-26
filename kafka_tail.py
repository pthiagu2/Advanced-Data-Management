from kafka import KafkaConsumer, TopicPartition
import argparse
import json
import os

CLIENT_ID = 'kafka-tail-{}'.format(os.getpid())


def run_kafka_tailer(kafka_server, kafka_topic, number):
    consumer = KafkaConsumer(bootstrap_servers=kafka_server,
                             client_id=CLIENT_ID,
                             group_id=CLIENT_ID)

    partitions = consumer.partitions_for_topic(kafka_topic)
    topic_partitions = list()
    for partition in partitions:
        topic_partitions.append(TopicPartition(kafka_topic, partition))

    consumer.assign(topic_partitions)
    consumer.seek_to_end()

    topic_partition_to_offset = dict()
    for topic_partition in topic_partitions:
        next_offset = consumer.position(topic_partition)
        reduced_offset = max(next_offset - number, 0)
        topic_partition_to_offset[topic_partition] = reduced_offset

    for topic_partition, offset in topic_partition_to_offset.items():
        consumer.seek(topic_partition, offset)

    count = 0
    for message in consumer:
        count += 1
        print('tail_count: {}, topic: {}'.format(count, kafka_topic))
        topic = message.topic
        partition = message.partition
        offset = message.offset
        timestamp = message.timestamp
        key = message.key

        try:
            value = json.loads(message.value.decode())
        except:
            value = message.value.decode()

        json_output = {
            'topic': topic,
            'partition': partition,
            'offset': offset,
            'timestamp': timestamp,
            'key': key,
            'value': value
        }
        print(json.dumps(json_output, indent=2, sort_keys=True))


def main():
    desc = 'Tail a Kafka Topic'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-s', '--kafkaserver', type=str,
                        default='localhost:9092',
                        help='URL of the Kafka server')
    parser.add_argument('-t', '--topicname', type=str,
                        help='Name of topic to read from')
    parser.add_argument('-n', '--number', type=int, default=10,
                        help='Number of messages to seek back before tailing')
    args = parser.parse_args()
    run_kafka_tailer(args.kafkaserver, args.topicname, args.number)


if __name__ == '__main__':
    main()
