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

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: SQL <file> ", file=sys.stderr)
        sys.exit(-1)
        
        
    spark = SparkSession\
        .builder\
        .appName("PythonSQL")\
        .getOrCreate()
    df = spark.read.load(sys.argv[1],
                     format=sys.argv[1][-3:], inferSchema="true", header="true")
    filtedDF = df.select(df['CCN'], df['REPORT_DAT'], df['OFFENSE'], df['METHOD'], df['END_DATE'], df['DISTRICT'])\
        .filter(df['CCN'].isNotNull() & \
            df['REPORT_DAT'].isNotNull() & \
            df['OFFENSE'].isNotNull() & \
            df['METHOD'].isNotNull() & \
            df['END_DATE'].isNotNull() & \
            df['DISTRICT'].isNotNull())
    filtedDF.groupBy("OFFENSE").count().show()
    filtedDF.withColumn("hour", sf.date_trunc('hour',sf.to_timestamp("timestamp","yyyy-MM-dd HH:mm:ss zz"))).show()
    spark.stop()
