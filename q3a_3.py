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
    if len(sys.argv) != 2:
        print("Usage: RageRank <file>", file=sys.stderr)
        sys.exit(-1)
   
        
        
    spark = SparkSession\
        .builder\
        .appName("PythonRageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    data = lines.filter(lambda x: x.encode("ascii", "ignore")[0]!='#')
    links = data.map(lambda x: x.split('\t'))
    keys = links.groupByKey()
    ranks = keys.map(lambda x: (x[0],1))

    contrib = links.join(ranks).map(lambda input:links.map(lambda dest: (dest,input[1][1]/links.size)))
    output3 = contrib.collect()
    for i in output3:
        print("out3 ",i)
    ranks = contrib.reduceByKey(lambda a, b: a + b).mapValues(lambda x: 0.15+0.85 * x)
    output2 = ranks.collect()
    for i in output2:
        print("out2 ",i)

    spark.stop()