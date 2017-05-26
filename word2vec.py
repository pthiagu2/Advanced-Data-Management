from pyspark.mllib.feature import Word2Vec
from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.feature import Word2VecModel

import sys

sc = SparkContext("local", "Simple App")
inp = sc.textFile(sys.argv[1]).map(lambda row: row.split(" "))
print inp
word2vec = Word2Vec()

#model = Word2VecModel.load(sc,"word2vec-model-new")
#print model

model = word2vec.fit(inp)

model.save(sc, "word2vec-model-new")

#synonyms = model.findSynonyms('Vector', 5)

#for word, cosine_distance in synonyms:
#    print("{}: {}".format(word, cosine_distance))