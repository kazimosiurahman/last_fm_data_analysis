#!/usr/bin/env python


from pyspark.sql.types import StructField, StructType, StringType , TimestampType,LongType
from etl_config import Sparksession,config


class LoadData(Sparksession):

    '''
    Initializes the Spark Session 
    I/P - AppName

    '''

    def __init__(self,appName,input_location=None):

        self.session = Sparksession(appName)

        self.session.enable_spark()

        self.data = None
        
        if input_location is None:
            
            self.input_location = config['input_location']
        else:
            self.input_location  = input_location
        
        self.schema = config['schema_definition']
        
        self.inferSchema = True

    def read_data(self):
        '''
        Reads the Data from Location present in the Config File
        Uses the Spark Session Initialised on the instance Level
        Reads a TSV file with delimeter as \t

        '''
       
        self.data = self.session.spark.read.option("sep","\t") \
                    .schema( schema= self.schema).csv(
                        self.input_location, 
                        inferSchema= self.inferSchema , 
                        header = False
                    )


        



