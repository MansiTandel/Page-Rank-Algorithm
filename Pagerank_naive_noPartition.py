import sys
import argparse
import numpy as np
from operator import add
from time import time
from pyspark import SparkContext
from pprint import pprint


def convergence_values(ro,rl,i,sc):
  rdd=sc.parallelize(ro)
  rdd=rdd.map(lambda x: (x[1],x[0]))
  rdd1=sc.parallelize(rl)
  rdd1=rdd1.map(lambda x: (x[1],x[0]))
  p=rdd.join(rdd1)
  # pprint(p.take(10))
  p=p.mapValues(lambda y: abs(y[1]-y[0]))
  p=p.values()
  val=p.reduce(lambda x,y:x+y)
  print('Iteration:{} Convergence:{}'.format(i+1,val))
  return val

def calculateContribs(urls, rank):
  num_urls = len(urls)
  for url in urls:
    yield (url, rank / num_urls)



def pageRank_Naive(file,iterations,sc):
  E=1e-4
  links = sc.textFile(file)
  # links.count()
  start_time = time()
  links=links.map(lambda x: (x.split('\t')[0],x.split('\t')[1])).distinct().groupByKey()
  # links.take(3)
  ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
  
  con_values=[]

  ro=ranks.values().zipWithIndex().collect()

  for j in range(iterations):
    contribs = links.join(ranks).flatMap(lambda x: calculateContribs(x[1][0], x[1][1]))
    contribs=contribs.sortBy(lambda x: x[1],ascending= False)
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    rl=ranks.values().zipWithIndex().collect()
    val=convergence_values(ro,rl,j,sc)
    con_values.append(val)
    ro=rl
    if(val<E):
      print('Ranks are converged at Iteration {} with value {}'.format(j+1,val))
    
 
    if(j==9 or j==19  or j==49 or j==99 or j==199 or val<E):
        list_of_ranks=ranks.collect()
        print('')
        print('Time taken for iteration {}'.format(j+1))
        pprint("--- %s seconds ---" % (time() - start_time))
        print('')
        print('----------10 Top ranked websites--------------')
        ranks=ranks.sortBy(lambda x:x[1],ascending=False)
        pprint(ranks.take(10))
        # ranks.values().sum()
        print('')
        print('----------10 Least ranked websites--------------')
        ranks=ranks.sortBy(lambda x:x[1],ascending=True)
        pprint(ranks.take(10))
        print('')
        print('-------Convergence values---------')
        print(con_values)

    if(val<E):
      break