#!/usr/bin/env python


############# HouseKeeping #####################

from etl_config import *
from load_data import LoadData
from transform import Transform

import os
import shutil

nameofApp = 'LastFManalysis'

#######################################

######  Creating Spark Session ########

sparkRead = LoadData( nameofApp )



############# Reading Data #############
sparkRead.read_data()



####### Creating Output directory ######


if os.path.isdir('output'):
    shutil.rmtree('./output')
    print("Deleted Old Folder")
    os.mkdir('./output')
else:
    os.mkdir('./output')




trans = Transform(sparkRead.data)

print("******************** The third task *************************")

trans.transform_q3()



print("******************** The second task *************************")


trans.transform_q2()


print("******************** The first task *************************")


trans.transform_q1()


print("**********************  Writing Files ************************")

trans.write_files()
