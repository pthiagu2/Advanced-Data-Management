Part 1: Kafka processing
-> The main files that use Kafka for processing are the kafka_produce_json.py (which reads the input tweets and writes all the tweets to mock_twitter_stream topic) and the kafka_middle.py (which reads from mock_twitter_stream topic and writes the tweets to spark_input topic after filtering the 9965 useful ones) 
-> The rest of the files kafka_consume.py are kafka_tail.py are just for testing purposes.
-> The entire kafka processing was setup on a local machine and hence localhost was used to access the server.

Part 2 : Spark processing
-> The file used to process tweets using Spark is Spark_streaming_script.py - the command for running this is given as comments at the top of the file.
-> This code on execution (with argument 1) will write to a collection named 'coll' in the mongodb database 'mydb'. 
-> This code on execution (with argument 0) will perform the clustering of the tweets and output the associated sentiments of the tweets in this cluster.

Part 3 : Mongodb querying
-> The queries used in mongodb were executed on the shell. The commands used are described in the pdf files and are also present in the mongodb_queries.sh file. 

