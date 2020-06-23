from pyspark import SparkConf, SparkContext
import json


conf = SparkConf().setMaster("local").setAppName("First")
sc = SparkContext(conf = conf)

punctuations = """.,:;'"!?"""


raw = sc.wholeTextFiles('./DATA-1')

raw2 = raw.mapValues(lambda x: x.lower())

raw3 = raw2.flatMapValues(lambda x: x.split()).distinct()

raw4 = raw3.mapValues(lambda x: x.strip(punctuations))

raw5 = raw4.map(lambda x: (x[1],x[0])).groupByKey()




#print(raw4.take(4))

#DEBUG
#word, filenames = raw3.take(3)[2]
#print(dir(filenames))
#print(filenames.data)

raw6 = raw5.mapValues(lambda x: len(x))
#print(raw5.take(3))

raw7 = raw6.collectAsMap()
bv = sc.broadcast(raw7)
print(bv.value)
