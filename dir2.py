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
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    import os
    for file in os.listdir(os.getcwd()):

        print(os.path.join(os.getcwd(), file)+"\n")
    for file in os.listdir(os.getcwd()+"/.."):
        print(os.path.join(os.getcwd(), file)+"\n")
        try:
            for file2 in os.listdir(os.getcwd()+"/../"+file):
                print(os.path.join(os.getcwd(), file2)+"\n")
                try:
                    for file3 in os.listdir(os.getcwd()+"/../"+file+"/"+file2):
                        print(os.path.join(os.getcwd(), file3)+"\n")
                except Exception as e: 
                    print(e)
        except Exception as e: 
            print(e)
    spark.stop()
