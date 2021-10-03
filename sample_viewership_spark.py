"""
file: viewership_main.py
brief: Loads viewership  csv file with Spark
author: Tolu O
date: December 23, 2019
"""
import datetime
import sys
import time
import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession
 
#from DB_Connection_script import insert_viewership
from spark_common import MATCH_SESSION_DATA_SCHEMA, viewership_TSV_READ_OPTIONS
from pyspark.sql import functions as f

 

def main():
    #CSV_FILE = '/home/hadoop/viewership_raw_files/03_viewership/'
    CSV_FILE = '/home/hadoop/viewership_raw_files/04_viewership/'

    # Method will iterate the Minimum date given
    def generatemin(v_mindate , v_maxdate):
        file_list = []
        v_mindate = datetime.datetime.strptime(v_mindate, '%Y-%m-%d %H:%M:%S')
        v_maxdate = datetime.datetime.strptime(v_maxdate, '%Y-%m-%d %H:%M:%S')
        timediff = v_maxdate - v_mindate
        
        # add first date to iteration
        file_list.append(v_mindate) 
        
        for i in range(round(timediff.seconds // 60)):

            v_mindate += datetime.timedelta(minutes=1)
            file_list.append(v_mindate)
        return file_list

    """The main driver for pulling data from the zipped viewership TSV objects"""
    spark_conf = SparkConf().setAppName("di_viewership_viewership_loader") 
        #.setMaster('yarn').set('spark.yarn.queue', 'batch') \
        #.set('spark.executor.cores', 5) \
        #.set('spark.dynamicAllocation.maxExecutors', 25) \
        #.set('spark.scheduler.mode', 'FAIR') \

    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    df = spark.read.format("csv").options(**viewership_TSV_READ_OPTIONS) \
        .load(CSV_FILE, schema=MATCH_SESSION_DATA_SCHEMA)

  

    # Drop unwanted columns
    df = df.drop("corrupt_record", "programAiringEndTime", "programAiringStartTime",
                 "dmaExtrapolated", "smoothed",
                 "endContentOffset", "startContentOffset")

    df.createOrReplaceTempView("df")
    df1 = spark.sql("""select deviceId ,  startSecondsTimeStamp, from_unixtime(startSecondsTimeStamp / 1000) as START_CNV \
                       ,endSecondsTimeStamp  , from_unixtime(endSecondsTimeStamp / 1000) as END_CNV \
                      ,channelTmsld , channelName  from df""")
  
    df1.show()
    
    # min and max minutes
    df1.createOrReplaceTempView("df1")

    mintime = spark.sql("SELECT  (MIN (START_CNV) ) As min_start_time   FROM df1")
    maxtime = spark.sql("SELECT  (max (START_CNV) ) As max_start_time   FROM df1")

    mintime = mintime.head()[0]
    maxtime = maxtime.head()[0]

    dfmin = generatemin(mintime, maxtime)

    # convert the array into a dataframe
    dfObj = pd.DataFrame(dfmin)
    dfs = spark.createDataFrame(dfObj)

    # Rename column
    ddfs = dfs.withColumnRenamed('0', 'MINUTE_START')

    minmax = ddfs.withColumn('MINUTE_END', (f.unix_timestamp("MINUTE_START") + 60).cast('timestamp'))

    minmax.createOrReplaceTempView("timeSpan")

    time_df = spark.sql("""SELECT MINUTE_START , MINUTE_END, unix_timestamp (MINUTE_START)*1000 as MINUTE_START_CNV, \
                           unix_timestamp (MINUTE_END) *1000  as MINUTE_END_CNV  from  timeSpan order by MINUTE_START asc """)
    time_df.createOrReplaceTempView("All_minutes")

    all_df = spark.sql('SELECT c.deviceId , c.channelName , m.minute_start , \
                                  RANK () OVER (PARTITION BY c.deviceId, m.minute_start  ORDER BY SUM \
                                 ( LEAST (c.endSecondsTimeStamp, m.MINUTE_END_CNV)  -  \
                                  GREATEST (c.startSecondsTimeStamp, m.MINUTE_START_CNV)) DESC) \
                                  AS rnk FROM  df1 c JOIN All_minutes m  ON c.START_CNV < m.MINUTE_END AND c.END_CNV > \
                                  m.MINUTE_START GROUP BY c.deviceId, c.CHANNELNAME, m.minute_start')
 


    all_df.createOrReplaceTempView("got_rnk")


    # get NIELSEN_WON_DEVICE_CNT
    dfNielsen = spark.sql('SELECT count(deviceId)  as NIELSEN_WON_DEVICE_CNT ,channelName,minute_start \
                             from got_rnk where rnk =1  GROUP BY minute_start, channelName ')
 
    dfNielsen.show()
    # get raw VIEWING_DEVICE_CNT
    dfRawView = spark.sql('SELECT count(deviceId)  as VIEWING_DEVICE_CNT ,channelName,minute_start \
                             from got_rnk  GROUP BY minute_start, channelName  ')

    dfJoin = dfRawView.join(dfNielsen, ['minute_start', 'channelName'], how="outer").orderBy(
        "minute_start")
    dfJoin.createOrReplaceTempView("dfJoin")

    finalrankDF = spark.sql('SELECT VIEWING_DEVICE_CNT,channelName as BROADCASTER , NIELSEN_WON_DEVICE_CNT,    \
                                    minute_start as MINUTE_START_TIMESTAMP , date_trunc("DD", minute_start) \
                                  as BROADCAST_DATE , "s_viewership_STG" as CREATED_BY  FROM \
                                    dfJoin')
    finalrankDF.show()
    
    # Call service to insert Data
    for i in finalrankDF.collect():
        insert_viewership(i.VIEWING_DEVICE_CNT, i.BROADCASTER, i.NIELSEN_WON_DEVICE_CNT,
                         i.MINUTE_START_TIMESTAMP, i.BROADCAST_DATE, i.CREATED_BY)

    #finalrankDF.write.format('jdbc').options(
      #url='jdbc:oracle:thin:@ldap://kpoid.***.***:2389/edw,cn=OracleContext,dc=***,dc=***',
      #driver='oracle.jdbc.driver.OracleDriver',
      #dbtable='*****._VIEWERSHIP',
      #user='*****_SCHEMA_APP',
      #password=' *******').mode('append').save()

if __name__ == '__main__':
    
    #   print("Starting execution at", datetime.now())
    main()
#  print("Execution finished in :", time.time() - START_TIME, "seconds at", datetime.now())
else:
    print('Usage: python3 viewership_main.py')
