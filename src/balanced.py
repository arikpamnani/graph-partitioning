import random
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)

k = 4	# number of partitions
part = []
for i in range(k):
	part.append(0)

test_file_name = "/home/arik/graph-partitioning/test.txt"
file_name = "/home/arik/graph-partitioning/database/Amazon0302.txt"

lines = sc.textFile(test_file_name)

def conv(x):
	x = x.split()
	return (int(x[0]), [int(x[1])])

edges = lines.map(lambda x: conv(x))
# print edges.collect()

# number of nodes
max_node = edges.max(lambda x: max(x[0], x[1]))
max_vertex = max_node[0]
# print max_vertex

# create an Adjacency list of the graph
AL = edges.reduceByKey(lambda a, b: a+b)
AL.persist()	# persist the AL RDD
AL_list = AL.collect()

# max_vertex -> maximum node id
# d -> dictionary (key = vertex of the graph, value = partition node)
d = {i:-1 for i in range(0, max_vertex+1)}
# print d

def graph_partition(v, k):
	minm = 999999
	for i in range(0, len(part)):
		if part[i] < minm:
			minm = part[i]
	choices = []	# all partitions with minimum number of partitions
	for i in range(0, len(part)):
		if(part[i] == minm):
			choices.append(i)
	# choose a random partition from the above choices 
	x = random.choice(choices)
	part[x] += 1
	d[v] = x
	return d[v]

# print d
# partition the nodes with graph_partition method
# wp -> AL (RDD) partitioned
wp = AL.partitionBy(k, lambda v: graph_partition(v, k))
print AL.collect()
# implement page rank algorithm 

