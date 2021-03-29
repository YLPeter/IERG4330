#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: RageRank <file> <num of iteration>", file=sys.stderr)
        sys.exit(-1)
    def computeContribs(dests, rank):
        num_urls = len(dests)
        for url in dests:
            yield (url, rank / num_urls)
        
        
    spark = SparkSession\
        .builder\
        .appName("PythonRageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    data = lines.filter(lambda x: x.encode("ascii", "ignore")[0]!='#')
    links = data.map(lambda x: x.split('\t'))
    links = links.groupByKey().cache()

    ranks = links.map(lambda x: (x[0],1))
    for i in range(int(sys.argv[2])):
        contribs = links.join(ranks).flatMap(lambda input: \
            computeContribs(input[1][0],input[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    output3 = ranks.sortBy(lambda x: x[1]).collect()
    for (link, rank) in output3[-1]:
        print('{:10s} has rank: {:20.10f}'.format(link.encode("ascii", "ignore"), rank))
    spark.stop()
