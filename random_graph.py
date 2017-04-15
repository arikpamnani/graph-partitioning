import random
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)

k = 100	# number of partitions
file_name = "/home/arik/Downloads/p2p-Gnutella04.txt"
lines = sc.textFile(file_name, k)

def graph_partition(v, k):
	# vertex v
	# ind -> {0, 1, 2, . . . , k-1}
	ind = v%k
	return ind

def conv(x):
	x = x.split()
	return (int(x[0]), [int(x[1])])

edges = lines.map(lambda x: conv(x))
# print edges.collect()

# number of nodes
max_node = edges.max(lambda x: max(x[0], x[1]))
# print max(max_node)

# create an Adjacency list of the graph
AL = edges.reduceByKey(lambda a, b: a+b)
wp = AL.partitionBy(k, lambda x: graph_partition(x, k))
print wp.glom().collect()

