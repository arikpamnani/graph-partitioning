import random
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)

k = 100	# number of partitions
part = []
for i in range(k):
	part.append([])
file_name = "/home/arik/graph-partitioning/p2p-Gnutella04.txt"
lines = sc.textFile(file_name, k)

def conv(x):
	x = x.split()
	return (int(x[0]), [[int(x[0]), int(x[1])]])

edges = lines.map(lambda x: conv(x))
# print edges.collect()

# number of nodes
max_node = edges.max(lambda x: max(x[0], x[1]))
# print max(max_node)

# create an Adjacency list of the graph
AL = edges.reduceByKey(lambda a, b: a+b)
# AL.persist()	# persist the AL RDD
AL_list = AL.glom().collect()

d = {i:-1 for i in range(max_node[0]+1)}
def graph_partition(v, k):
	for i in range(len(AL_list)):
		print AL_list[i][0]
graph_partition(2, k)

# wp = AL.partitionBy(k, lambda x: graph_partition(x, k))
