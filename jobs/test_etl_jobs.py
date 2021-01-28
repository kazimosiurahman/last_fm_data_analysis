#!/usr/bin/env python


############# HouseKeeping #####################

from test_etl_config import *

from load_data import LoadData
from transform import Transform
from pandas.testing import assert_frame_equal
import pandas as pd

import os
import shutil


class UnitTest():

    def __init__(self,nameofApp) :

        self.sparkSess = None
        self.trans = None
        self.nameofApp = nameofApp
        self.success = 0
        self.failure = 0


    def test_loadData(self):
 

        self.sparkSess = LoadData( self.nameofApp , input_location = config['input_location'] )

        self.sparkSess.read_data()

        try :
            assert( self.sparkSess.data.count() != 0 )

            print('--------- Successfull Run LoadData ------------')
            self.success += 1

        except AssertionError as msg:  

            self.failure +=1

            print(msg)
            



    def test_transformq1(self):

        self.trans = Transform(self.sparkSess.data)

        self.trans.transform_q1()

        try:

            assert_frame_equal(
                self.trans.output1.toPandas().applymap(str) , 
                pd.read_csv(config['output_location1'],sep='\t').applymap(str))

            print('--------- Successfull Run Transformq1 ------------')
            self.success += 1

        except AssertionError as msg:  
            print(msg) 
            self.failure +=1

    def test_transformq2(self):

        self.trans = Transform(self.sparkSess.data)
        
        self.trans.transform_q2()

        try :

            assert_frame_equal(self.trans.output2.toPandas() ,
                                pd.read_csv(config['output_location2'] ,sep='\t'))

            print('--------- Successfull Run Transformq2 ------------')
            self.success += 1

        except AssertionError as msg:  
            print(msg) 
            self.failure +=1

    def test_transformq3(self):

        self.trans = Transform(self.sparkSess.data)
        
        self.trans.transform_q3()
        try :
            assert_frame_equal(self.trans.output3.toPandas() ,
                             pd.read_csv(config['output_location3'] ,sep='\t'))

            print('--------- Successfull Run Transformq3 ------------')
            self.success += 1


        except AssertionError as msg:  
            print(msg) 
            self.failure +=1


if __name__ == '__main__' :

    unitTest = UnitTest('UnitTesting')

    unitTest.test_loadData()


    unitTest.test_transformq1()

    unitTest.test_transformq2()

    unitTest.test_transformq3()

    print(f'Number of Successful Run {unitTest.success} ')

    print(f'Number of Failed Run {unitTest.failure} ')