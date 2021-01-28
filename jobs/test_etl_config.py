#!/usr/bin/env python


from pyspark.sql import SparkSession

from pyspark.sql.types import StructField, StructType, StringType , TimestampType,LongType

config = {
    'input_location'   : './test_output/Input_test.tsv',
    'output_location1' : './test_output/Output1.tsv',
    'output_location2' : './test_output/Output2.tsv',
    'output_location3' : './test_output/Output3.tsv',
    'schema_definition' :   StructType() \
                                        .add("userId",StringType(),True) \
                                        .add("timestamp",TimestampType(),True) \
                                        .add("artId",StringType(),True) \
                                        .add("artName",StringType(),True) \
                                        .add("traid",StringType(),True) \
                                        .add("SongName",StringType(),True) 
}


class Sparksession():


    def __init__(self,appName):

        self.appName = appName
        self.spark = None
        self.session = None

    def enable_spark(self):

        self.spark = SparkSession.builder\
                                    .appName(self.appName) \
                                    .getOrCreate()


    






