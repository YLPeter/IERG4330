from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *


if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("sys.argv")
        for i in range(len(sys.argv)):
            print(i,sys.argv[i])
    def computeContribs(dests, rank):
        num_urls = len(dests)
        for url in dests:
            yield (url, rank / num_urls)
    spark = SparkSession\
        .builder\
        .appName("PythonSQL")\
        .getOrCreate()
        
    df_mooc = spark.read.csv(sys.argv[1], sep=r'\t', header=True)
    df_mooc = df_mooc.select(df_mooc['USERID'], df_mooc['TARGETID'], df_mooc['TIMESTAMP'])
    df_vertices = spark.read.csv(sys.argv[2], sep=r'\t', header=True)
    df_vertices = df_vertices.select(df_vertices['id'], df_vertices['type'])
    
    ans1_total = df_vertices.count()
    print("ans1_total:",ans1_total)
    
    ans2_total = df_vertices.filter(df_vertices['type'] == "User").count()
    print("ans2_total:",ans2_total)
    ans3_total = df_vertices.filter(df_vertices['type'] == "Course Activity").count()
    print("ans3_total:",ans3_total)
    ans4_total = df_mooc.count()
    print("ans4_total:",ans4_total)
    ans5_total = df_mooc.groupBy("TARGETID").count().sort(desc("count")).show()
    ans6_total = df_mooc.groupBy("USERID").count().sort(desc("count")).show()
    #filtedDF = df_mooc.select(df_mooc['USERID'], df_mooc['TARGETID'], df_mooc['TIMESTAMP']).show()
    
    #df = spark.read.csv(sys.argv[1], sep=r'\t', header=True)
    #filtedDF = df.select(df['USERID'], df['TARGETID'], df['TIMESTAMP']).show()
    """filtedDF = df.select(df['CCN'], df['REPORT_DAT'], df['OFFENSE'], df['METHOD'], df['END_DATE'], df['DISTRICT'])\
        .filter(df['CCN'].isNotNull() & \
            df['REPORT_DAT'].isNotNull() & \
            df['OFFENSE'].isNotNull() & \
            df['METHOD'].isNotNull() & \
            df['END_DATE'].isNotNull() & \
            df['DISTRICT'].isNotNull())
    filtedDF.groupBy("OFFENSE").count().show()
    df.filter(df['SHIFT'].isNotNull()).groupBy("SHIFT").count().show()"""


    spark.stop()
