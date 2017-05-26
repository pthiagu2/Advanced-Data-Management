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

PORT = int(sys.argv[1])

BATCH_INTERVAL = 20  # How frequently to update (seconds)
WINDOWS_LENGTH=60  #the duration of the window
SLIDING_INTERVAL=20 #the interval at which the window operation is performed
clusterNum=15 #Number of CLusters

# Get json object from json text. 
def get_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  return json_object

# Create a vector for a document by averaging the word vectors.
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


# regex to remove special characters
remove_spl_char_regex = re.compile('[%s]' % re.escape(string.punctuation)) 
vocab = {}
# List of stop words. 
stopwords=[u'get',u'amp',u'rt', u're', u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves', u'you', u'your', u'yours', u'yourself', u'yourselves', u'he', u'him', u'his', u'himself', u'she', u'her', u'hers', u'herself', u'it', u'its', u'itself', u'they', u'them', u'their', u'theirs', u'themselves', u'what', u'which', u'who', u'whom', u'this', u'that', u'these', u'those', u'am', u'is', u'are', u'was', u'were', u'be', u'been', u'being', u'have', u'has', u'had', u'having', u'do', u'does', u'did', u'doing', u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'because', u'as', u'until', u'while', u'of', u'at', u'by', u'for', u'with', u'about', u'against', u'between', u'into', u'through', u'during', u'before', u'after', u'above', u'below', u'to', u'from', u'up', u'down', u'in', u'out', u'on', u'off', u'over', u'under', u'again', u'further', u'then', u'once', u'here', u'there', u'when', u'where', u'why', u'how', u'all', u'any', u'both', u'each', u'few', u'more', u'most', u'other', u'some', u'such', u'no', u'nor', u'not', u'only', u'own', u'same', u'so', u'than', u'too', u'very', u's', u't', u'can', u'will', u'just', u'don', u'should', u'now']

# Returns a set of words from a tweet
# Filter stopwords, punctuations, etc.
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
#and word in vocab \
            
# Returns a set of 5 most common terms. 
def freqcount(terms_all):
    count_all = Counter()
    count_all.update(terms_all)
    return count_all.most_common(5)

# Set up spark objects and run

sc = SparkContext("local[*]", "Simple App")
sc.setLogLevel("FATAL")
sqlContext=SQLContext(sc)
# Read work2vec model
lookup = sqlContext.read.parquet("word2vec-model-new/data").alias("lookup")
lookup.printSchema()

# To load Word2Vec model in a low cost manner, we use a Broadcast variable to keep the model on each machine as
# a cached and read-only variable, rather than shipping a copy of it with tasks.
# It will give every node a copy of the model efficiently.

word2vecModel_broadCast = sc.broadcast(lookup.rdd.collectAsMap())
# To setup the streaming data
ssc = StreamingContext(sc, BATCH_INTERVAL)


# Create a DStream that will connect to hostname:port
dstream = ssc.socketTextStream("localhost", PORT)
dstream_tweets=dstream.map(lambda post: get_json(post))\
     .filter(lambda post: post != False)\
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


# First, get a list of all words assigned to a single cluster using the reduceByKey function. 
# Then, it gets the top-5 most frequent words in that cluster. 
# Finally, topicAgg contains (cluster label, top-5 words) for each cluster.
topicAggregate = topic.reduceByKey(lambda x,y: x+y).map(lambda x: (x[0],freqcount(x[1])))
topicAggregate.pprint()


def outputClust(d):
    for c in d:
        print (c)
#topic.foreachRDD(lambda time, rdd: print(rdd.collect()))

#clust.foreachRDD(lambda time, rdd: print(rdd.collect()))
#textdata.foreachRDD(lambda time, rdd: print(rdd.collect()))
#clust.foreachRDD(lambda time, rdd: q.put(rdd.collect()))

ssc.start()
ssc.awaitTermination()


