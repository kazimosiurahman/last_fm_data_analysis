#!/usr/bin/env python


from pyspark.sql.types import StructField, StructType, StringType , TimestampType,LongType
from etl_config import Sparksession,config


class LoadData(Sparksession):

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
       
            self.data = self.session.spark.read.option("sep","\t") \
                        .schema( schema= self.schema).csv(
                            self.input_location, 
                            inferSchema= self.inferSchema , 
                            header = True
                        )


        



