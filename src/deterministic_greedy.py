import time
import random
import re
from operator import add
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)

k = 4	# number of partitions
part = []
for i in range(k):
	part.append([])


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def intersection(A, B):
	setA = set(B)
	C = [val for val in A if val in setA]
	return C

test_file_name = "/home/arik/graph-partitioning/database/test.txt"
file_name = "/home/arik/graph-partitioning/database/p2p-Gnutella04.txt"

lines = sc.textFile(file_name)

def conv(x):
	x = x.split()
	return (int(x[0]), [int(x[1])])

edges = lines.map(lambda x: conv(x))

# number of nodes
max_node = edges.max(lambda x: max(x[0], x[1]))
max_vertex = max_node[0]
# print max_vertex

# create an Adjacency list of the graph
AL = edges.reduceByKey(lambda a, b: a+b)
AL.persist()	# persist the AL RDD
AL_list = AL.collect()
# print AL_list

# d -> dictionary (key = vertex of the graph, value = partition node)
d = {i:-1 for i in range(0, max_vertex+1)}
KV = AL.collectAsMap()	# export AL RDD as Key-Value pairs
# print KV
def graph_partition(v, k):
	maxm = -1
	# neighbour list of v
	# edges_current = AL.lookup(v)[0]
	edges_current = KV[v]
	for i in range(len(part)):
		C = intersection(edges_current, part[i])
		if(len(C) > maxm):
			maxm = len(part[i])
	choices = []
	for i in range(len(part)):
		if(len(part[i]) == maxm):
			choices.append(i)
	x = random.choice(choices)
	part[x].append(v)
	d[v] = x
	return d[v]
wp = AL.partitionBy(k, lambda v: graph_partition(v, k))
# print d
# print wp.glom().collect()

# PAGE RANK #

ranks = wp.map(lambda x: (x[0], 1.0))
# print ranks.collect()
x = wp.join(ranks)
# print x.collect()
number_of_iterations = 1
for iteration in range(number_of_iterations):
	contribs = x.flatMap(lambda x: computeContribs(x[1][0], x[1][1]))
	ranks = contribs.reduceByKey(add)	# .mapValues(lambda rank: rank * 0.85 + 0.15)
	ranks_ = ranks.mapValues(lambda rank: rank * 0.85 + 0.15)
for (link, rank) in ranks_.collect():
	print ("%s has rank: %s" % (link, rank))

sc.stop()



