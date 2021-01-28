#!/usr/bin/env python

from pyspark.sql import functions as F
from pyspark.sql import Window as w
from etl_config import config

class Transform():


        def __init__(self,data):

                self.data = data
                self.output1 = None
                self.output2 = None
                self.output3 = None


        def transform_q3(self):

                '''
                Create a list of user IDs, along with the number of distinct songs each user has played.
                '''

                self.output3 = self.data.groupby('userId')\
                                        .agg(F.countDistinct('SongName'))\
                                        .orderBy('userId')\
                                        .select(F.col('userId'),F.col('count(SongName)').alias('Distinct Songs'))
   



        def transform_q2(self):
                '''
                Create a list of the 100 most popular songs (artist and title) in the dataset, with the number
                of times each was played.

                '''
                
                self.output2 = self.data.groupby(['artname','SongName'])\
                        .count()\
                        .sort(F.col('count').desc())\
                        .select(F.col('artName').alias('artist'),F.col('SongName').alias('title'),F.col('count').alias('Frequency'))\
                        


        def transform_q1(self):

                '''
                Say we define a user’s “session” of Last.fm usage to be comprised of one or more songs
                played by that user, where each song is started within 20 minutes of the previous song’s
                start time. Create a list of the top 10 longest sessions, with the following information about
                each session: userid, timestamp of first and last songs in the session, and the list of songs
                played in the session (in order of play).


                '''
                ## Phase 1 helps in creating the time difference in minutes between two Successive Plays

                phase_1 = self.data.orderBy(['userId','timestamp'])\
                                        .withColumn("leadtimestamp", 
                                                        F.lag('timestamp',1)
                                                                        .over(
                                                                                w.partitionBy('userId').orderBy('userId')
                                                                        )
                                        ).withColumn(
                                                        "date_diff_min", 
                                                        (F.col("timestamp").cast("long") - F.col("leadtimestamp").cast("long"))/60.00
                                ).fillna({'date_diff_min': 0})



                ## Phase 2 ranks the Succesive Play Time Difference and filters out the Time Diff < 20 minutes 

                phase_2 = phase_1.withColumn('ranks',
                                F.row_number()
                                .over
                                (
                                w.partitionBy('userId').orderBy('timestamp')
                                )
                                ).filter(F.col("date_diff_min") < 20)


                ## Phase 3 Ranks the previously Ranked List to generate a logic for counting successive session less than 20 minutes
                ## to be grouped by
                phase_3 = phase_2.withColumn('groups', 
                        F.col('ranks') - \
                        F.row_number()
                        .over(
                        w.partitionBy('userId').orderBy('ranks')
                        )
                )


                ## the Difference between the two Ranks groups the Successive Songs played within 20 minute difference 

                self.output1 = phase_3.groupBy(['userId','groups'])\
                        .agg(
                                F.collect_set("SongName").alias('list of Songs'),
                                F.min('timestamp').alias('start_session'),
                                F.max('timestamp').alias('end_session'),
                                F.round(F.sum('date_diff_min'),0).alias('total_session_inMins')
                        )\
                        .sort(F.col('total_session_inMins').desc())\
                        .drop('groups')




        def write_files(self):

                '''
                Writing the Files in an Output Folder based on the location provided in
                the Config Files
                
                '''

                self.output3.toPandas().to_csv(config['output_location3'],sep='\t')


                self.output2.limit(100).toPandas().to_csv(config['output_location2'],sep='\t')

                self.output1.toPandas().to_csv(config['output_location1'],sep='\t')