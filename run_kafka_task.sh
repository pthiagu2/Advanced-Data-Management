#python kafka_consume.py & # This is a file to test if the spark_input topic has the 9965 tweets. 
python kafka_middle.py & # (Task 2) This python file reads from mock_twitter_stream topic 
#and writes the tweets to spark_input topic after filtering the 9965 useful ones.
python kafka_produce_json.py & # (Task 1) This python file reads the tweets from tweets.txt 
#and writes all the tweets to mock_twitter_stream topic. 

#python kafka_tail.py -t mock_twitter_stream -n 0 # Given python file to test. 


# This code assumes the raw_data/ folder is placed in the same path as this script. 