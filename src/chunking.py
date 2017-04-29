import time
import re
import sys
from operator import add
from pyspark import SparkConf, SparkContext

start_time = time.time()

k = 4
conf = SparkConf().setMaster("spark://arik-HP-Pavilion-15-Notebook-PC:7077").setAppName("test")
sc = SparkContext(conf = conf)

def conv(x):
	x = x.split()
	return (int(x[0]), [int(x[1])])

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]
    
lines = sc.textFile('/home/arik/graph-partitioning/database/Amazon0302.txt', k)

# Loads all URLs from input file and initialize their neighbors.
# links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
links = lines.map(lambda x: conv(x))

ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

x = links.join(ranks)
# print x.glom().collect()
# print x.collect()

for iteration in range(int(2)):
	# Calculates URL contributions to the rank of other URLs.
	contribs = x.flatMap(
		lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
	ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

for (link, rank) in ranks.collect():
	print("%s has rank: %s." % (link, rank))

print (time.time() - start_time)
# sc.stop()
