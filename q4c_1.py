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
import pyspark.sql.functions as sf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: SQL <file> <file> ", file=sys.stderr)
        sys.exit(-1)
    def filtering(df):
        return df.select(df['METHOD'])\
        .filter(df['METHOD'] == "GUN")\
            .count()
         
    spark = SparkSession\
        .builder\
        .appName("PythonSQL")\
        .getOrCreate()
    df = spark.read.load(sys.argv[1],
                     format=sys.argv[1][-3:], inferSchema="true", header="true")
    df2 = spark.read.load(sys.argv[2],
                     format=sys.argv[1][-3:], inferSchema="true", header="true")
    offenseCount = filtering(df)
    offenseCount2 = filtering(df2)
    result = offenseCount.union(offenseCount2)
    result.count().show()
    offenseCount.show()
    offenseCount2.show()
    result.show()
    
    spark.stop()
