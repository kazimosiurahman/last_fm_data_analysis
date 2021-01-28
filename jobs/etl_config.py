#!/usr/bin/env python


from pyspark.sql import SparkSession

from pyspark.sql.types import StructField, StructType, StringType , TimestampType,LongType

config = {
    'input_location'   : '../lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv',
    'output_location1' : './output/output1.tsv',
    'output_location2' : './output/output2.tsv',
    'output_location3' : './output/output3.tsv',
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
        '''
        Creates a Spark Session

        '''
        self.spark = SparkSession.builder\
                                    .appName(self.appName) \
                                    .getOrCreate()


    






