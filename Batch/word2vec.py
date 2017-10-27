import sys
import time
import json
from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

# Generate word2evtor model base on keywords of ads


training_file = sys.argv[1]
synonyms_data_file = sys.argv[2]

sc = SparkContext(appName="word2vec")
inp = sc.textFile(training_file).map(lambda line: line.encode("utf8", "ignore").split(" "))

word2vec = Word2Vec()
#millis = int(round(time.time() * 1000))
#model = word2vec.setMinCount(5).setVectorSize(10).setSeed(2017).fit(inp)
#model = word2vec.setVectorSize(10).setSeed(2017).fit(inp)
model = word2vec.setLearningRate(0.02).setMinCount(5).setVectorSize(10).setSeed(2017).fit(inp)

#model = word2vec.setMinCount(5).setVectorSize(10).setSeed(2017).fit(inp)

vec = model.getVectors()
synonyms_data = open(synonyms_data_file, "w")

print "len of vec", len(vec)
for word in vec.keys():
    synonyms = model.findSynonyms(word, 5)
    entry = {}
    entry["word"] = word
    synon_list = []
    for synonym, cosine_distance in synonyms:
        synon_list.append(synonym)
    entry["synonyms"] = synon_list
    synonyms_data.write(json.dumps(entry))
    synonyms_data.write('\n')

synonyms_data.close()

#print vec
test_data = ["dslr", "furniture", "shaver", "toddler", "sport","powershot", "xbox", "led"]
for w in test_data:
    synonyms = model.findSynonyms(w, 5)
    print "synonyms of ",w
    for word, cosine_distance in synonyms:
        print("{}: {}".format(word, cosine_distance))

model.save(sc, "../data/model/word2vec_new3")
sc.stop()
