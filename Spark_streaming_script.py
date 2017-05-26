# This file is run using the following command :
'''
 spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar 
--conf "spark.mongodb.input.uri=mongodb://localhost/mydb.mycollection?readPreference=primaryPreferred" 
--conf "spark.mongodb.output.uri=mongodb://localhost/mydb.mycollection" 
--packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0 Spark_streaming_script.py <arg>'''

# This code assumes that the kafka-spark connector jar file - spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar is present in the same path
# and also assumes that spark-mongodb connector has been installed.

# This code takes one input argument. If the argument is 0, then the spark analysis tasks executes. 
# If the argument is 1, then the mongodb output takes place. 
from __future__ import print_function
import os
import sys
import json
import re
import string
import numpy as np
from collections import Counter
from pyspark import SparkContext
from pyspark import SQLContext,Row
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import HashingTF,IDF, Tokenizer
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.feature import Word2VecModel
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
import codecs
from pyspark.sql import SparkSession
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk


clusterNum=15
stopwords=[u'get',u'amp',u'rt', u're', u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves', u'you', u'your', u'yours', u'yourself', u'yourselves', u'he', u'him', u'his', u'himself', u'she', u'her', u'hers', u'herself', u'it', u'its', u'itself', u'they', u'them', u'their', u'theirs', u'themselves', u'what', u'which', u'who', u'whom', u'this', u'that', u'these', u'those', u'am', u'is', u'are', u'was', u'were', u'be', u'been', u'being', u'have', u'has', u'had', u'having', u'do', u'does', u'did', u'doing', u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'because', u'as', u'until', u'while', u'of', u'at', u'by', u'for', u'with', u'about', u'against', u'between', u'into', u'through', u'during', u'before', u'after', u'above', u'below', u'to', u'from', u'up', u'down', u'in', u'out', u'on', u'off', u'over', u'under', u'again', u'further', u'then', u'once', u'here', u'there', u'when', u'where', u'why', u'how', u'all', u'any', u'both', u'each', u'few', u'more', u'most', u'other', u'some', u'such', u'no', u'nor', u'not', u'only', u'own', u'same', u'so', u'than', u'too', u'very', u's', u't', u'can', u'will', u'just', u'don', u'should', u'now']

# Returns a set of words from a tweet
# Filter stopwords, punctuations, etc.
remove_spl_char_regex = re.compile('[%s]' % re.escape(string.punctuation)) 
def tokenize(tweet):
    tokens = []
    tweet = tweet.encode('ascii', 'ignore') #to decode
    # to replace url with ''
    tweet=re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', tweet) 
    tweet = remove_spl_char_regex.sub(" ",tweet)  # Remove special characters
    tweet=tweet.lower()
    for word in tweet.split():
        if word not in stopwords \
            and word not in string.punctuation \
            and len(word)>1 \
            and word != '``':
                tokens.append(word)
    return tokens

def doc2vec(document):
    doc_vec = np.zeros(100)
    tot_words = 0
    for word in document:
        try:
            vec = np.array(word2vecModel_broadCast.value.get(word))
            if vec is not None:
                doc_vec +=  vec
                tot_words += 1
        except:
            continue
    if tot_words == 0:
        return doc_vec 
    else:
        return doc_vec / float(tot_words)

vader_analyzer = SentimentIntensityAnalyzer()
zk = "localhost:2181" # Zookeeper host
reload(sys)
sys.setdefaultencoding('utf-8')

BATCH_INTERVAL = 0.01

sc = SparkContext("local[*]", "Simple App")
sc.setLogLevel("FATAL")
ssc = StreamingContext(sc, BATCH_INTERVAL)

sqlContext=SQLContext(sc)
lookupVec = sqlContext.read.parquet("word2vec-model-new/data").alias("lookup")
lookupVec.printSchema()
# To load Word2Vec model in a low cost manner, we use a Broadcast variable to keep the model on each machine as
# a cached and read-only variable, rather than shipping a copy of it with tasks.
# It will give every node a copy of the model efficiently.

word2vecModel_broadCast = sc.broadcast(lookupVec.rdd.collectAsMap())



kafkaStream = KafkaUtils.createStream(ssc,zk, "spark-streaming-consumer", {"spark_input":1})
lines = kafkaStream.map(lambda x: x[1])
json_objects = lines.map(lambda tpl: json.loads(tpl)) # Get json objects from kafka stream

def outputToMongod(js):
	if len(js) == 0:
		return
	tweets_json = []
	js_new = []
	js.pprint()
	for x in js:
		js_dict = {}
		sents = vader_analyzer.polarity_scores(x['text'])
		x['positive_sentiment'] = sents['pos']
		x['negative_sentiment'] = sents['neg']
		x['neutral_sentiment'] = sents['neu']
		js_new.append(json.dumps(x))
	
	rdd  = sc.parallelize(js_new) # Create an rdd with json objects
	lookup = sqlContext.read.json(rdd) # Create a data frame from json objects
	lookup.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database",
	"mydb").option("collection", "coll").save()

def red(x,y):
	#print (x, y)
	for i in range(0, len(y)):
		x[i] += y[i]
	return x
def clusterSentimentAnalysis(json_objects):
	dstream_tweets=json_objects.filter(lambda post: post != False)\
	     .filter(lambda post: "text" in post)\
	     .map(lambda post: post["text"])\
	     .filter(lambda tpl: len(tpl) != 0)\
	     .map(lambda tpl: tokenize(tpl))\
	     .filter(lambda tpl: len(tpl) != 0)\
	     .map(lambda tpl:(tpl,doc2vec(tpl)))
	# dstream_tweets has ([w1, w2, ...], vectors of all words)
	trainingData=dstream_tweets.map(lambda tpl: tpl[1].tolist())
	# trainingData has [coord1, coord2, vectors of all words]
	testdata=dstream_tweets.map(lambda tpl: (tpl[0],tpl[1].tolist()))
	# testdata has ( ([words]), [vectors of all words] )
	model = StreamingKMeans(k=clusterNum, decayFactor=0.6).setRandomCenters(100, 1.0, 3)
	model.trainOn(trainingData)
	clust=model.predictOnValues(testdata)
	# clust has a list of tuples - ( ([words]), cluster label )
	topic=clust.map(lambda x: (x[1],x[0]))
	# Topic has (cluster label, [words]) for each tweet.
	topic.pprint()
	topicSents = topic.map(lambda x: (x[0],[vader_analyzer.polarity_scores(' '.join(x[1]))['pos'],vader_analyzer.polarity_scores(' '.join(x[1]))['neg'],vader_analyzer.polarity_scores(' '.join(x[1]))['neu'] ]))
	# x[1]
	topicSents.pprint()
	#topicSentsCount = topicSents.reduceByKey(lambda x,y : x+1)
	#topicSentsCount.pprint()
	topicSentsAverage = topicSents.reduceByKey(lambda x,y : red(x,y))
	topicSentsAverage.pprint()
arg = sys.argv[1]
if arg == 0:
	clusterSentimentAnalysis(json_objects)
else:
	json_objects.foreachRDD(lambda js: outputToMongod(js.collect()))

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mydb.mycollection") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mydb.mycollection") \
    .getOrCreate()

ssc.start()
ssc.awaitTermination()
