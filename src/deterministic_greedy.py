import random
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)

k = 100	# number of partitions
part = []
for i in range(k):
	part.append([])
file_name = "/home/arik/graph-partitioning/test.txt"
lines = sc.textFile(file_name)

def conv(x):
	x = x.split()
	return (int(x[0]), [[int(x[0]), int(x[1])]])

edges = lines.map(lambda x: conv(x))
# print edges.collect()

# number of nodes
max_node = edges.max(lambda x: max(x[0], x[1]))
max_vertex = max(max_node[1][0])

# create an Adjacency list of the graph
AL = edges.reduceByKey(lambda a, b: a+b)
AL.persist()	# persist the AL RDD
AL_list = AL.collect()

# max_vertex -> maximum node id
d = {i:-1 for i in range(0, max_vertex+1)}

def graph_partition(v, k):
	maxm = max_vertex+1
	choices = []	
	for i in range(len(part)):
		if(len(part[i]) < maxm):
			maxm = len(part[i])
			choices.append(i)
	for i in range(len(part)):
		if()
graph_partition(2, k)

# wp = AL.partitionBy(k, lambda x: graph_partition(x, k))
