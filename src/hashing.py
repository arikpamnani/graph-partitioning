import time
import random
import re
from operator import add
from pyspark import SparkConf, SparkContext

start_time = time.time()

conf = SparkConf().setMaster("local").setAppName("test")
# conf = SparkConf().setMaster("spark://arik-HP-Pavilion-15-Notebook-PC:7077").setAppName("test")
sc = SparkContext(conf = conf)

k = 4	# number of partitions
file_name = "/home/arik/graph-partitioning/database/Amazon0302.txt"
lines = sc.textFile(file_name, k)
# print lines.collect()
def graph_partition(v, k):
	# vertex v
	# ind -> {0, 1, 2, . . . , k-1}
	ind = v%k
	return ind

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def conv(x):
	x = x.split()
	return (int(x[0]), [int(x[1])])

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

# Adjacency list
edges = lines.map(lambda x: conv(x))
# print edges.collect()

# number of nodes
max_node = edges.max(lambda x: max(x[0], x[1]))
# print max(max_node)

# create an Adjacency list of the graph
AL = edges.reduceByKey(lambda a, b: a+b)
# print AL.collect()
# key -> vertex, value -> list of edges with which the vertex is associated
wp = AL.partitionBy(k, lambda x: graph_partition(x, k))
# print wp.glom().collect()

# PAGE RANK #

ranks = wp.map(lambda x: (x[0], 1.0))
# print ranks.collect()
x = wp.join(ranks)
# print x.collect()
number_of_iterations = 20
for iteration in range(int(number_of_iterations)):
	contribs = x.flatMap(lambda x: computeContribs(x[1][0], x[1][1]))
	ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

for (link, rank) in ranks.collect():
	print ("%s has rank: %s" % (link, rank))

# print (time.time() - start_time)
sc.stop()












