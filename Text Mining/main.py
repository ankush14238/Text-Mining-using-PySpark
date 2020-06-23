from pyspark import SparkConf, SparkContext
import json
from math import log
import os


conf = SparkConf().setMaster("local").setAppName("First")
sc = SparkContext(conf = conf)

punctuations = ".,:;'!?"

# TF Computation

rawdata = sc.wholeTextFiles('./DATA-1')
lines = rawdata.filter(lambda element: element[1]!='')
stop = sc.textFile('stopwords.txt')
word = lines.flatMapValues(lambda word: word.split())
word = word.mapValues(lambda element: (element.lower()))
word = word.mapValues(lambda element: element.strip(punctuations))

def readFile(filename):
	words = []
	with open('stopwords.txt','r') as infile:
		for line in infile:
			words.append(line.strip())

	return words

stopwords = sc.broadcast(readFile('stopwords.txt'))


newwords = word.filter(lambda element: element[1] not in stopwords.value)


counts = newwords.map(lambda element: (element,1)).reduceByKey(lambda x,y: x+y).filter(lambda element: element[1]>=2)
print("================ TERM FREQUENCIES ================")
print(counts.take(50))
print("==================================================")


# ((word, document), TF)

# IDF computation

raw = sc.wholeTextFiles('./DATA-1')

raw2 = raw.mapValues(lambda x: x.lower())

raw3 = raw2.flatMapValues(lambda x: x.split())

raw4 = raw3.mapValues(lambda x: x.strip(punctuations)).distinct()

raw5 = raw4.map(lambda x: (x[1],x[0])).groupByKey()

#print(raw4.take(4))

#DEBUG
#word, filenames = raw3.take(3)[2]
#print(dir(filenames))
#print(filenames.data)

raw6 = raw5.mapValues(lambda x: len(x))
#print("DF(the): ", raw6.lookup("the"))

N = sc.broadcast(raw.count())
#print("N: ", N.value)

raw8 = raw6.map(lambda element: (element[0],log(N.value)/float(element[1])))
print("================ ID FREQUENCIES ================")
print(raw8.take(50))
print("==================================================")

raw9 = raw8.collectAsMap()

idf = sc.broadcast(raw9)

tfidf = counts.map(lambda element: (element[0], element[1]*idf.value[element[0][1]]))

final40 = []
for x in os.listdir('./DATA'):
	tfidf = tfidf.map(lambda element: ((os.path.basename(element[0][0]), element[0][1]), element[1]))
	final40.extend(tfidf.top(5, key = lambda element: element[1] if element[0][0] == x else 0))
print(final40)
result = {}
for element in final40:
	(document, word), count = element
	result[word] = count


with open('./sp4(TF-IDF).json' , 'w') as fp:
	json.dump(result, fp, indent = 2)

#countscore = counts.map(lambda element: (element, element[1]*idf.value[element]))



#print(counts.top(40,key = lambda element: element[1]))
#print(raw8.take(40))




# Take Top-x

#finalcount = countscore.top(40, key= lambda element: element[1])



#result = {}
#for element in finalcount:
		#word, count = element
		#result[word] = count




#print(counts.take(2))

#counts.toJSON('~/first.json')
#with open('./sp4.json' , 'w') as fp:
	#json.dump(result, fp, indent = 2)

