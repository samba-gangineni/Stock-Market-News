from __future__ import print_function
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit, to_date, sum
import sys

__author__ = "Sambasiva Rao Gangineni"

reload(sys)
sys.setdefaultencoding('utf8')


'''
    This function returns the index of the url,title and dop
'''
def urlAndTitle(x):
    url_index = x.index('"url":')
    title_index = x.index('"title":')
    dop_index = x.index('"dop":')
    text_index = x.index('"text":')
    
    url_from = url_index+7
    url_to = title_index-2
    title_from = title_index+9
    title_to = dop_index-2
    dop_from = dop_index+7
    dop_to = text_index-2

    url = x[url_from:url_to]
    title = x[title_from:title_to]
    dop = x[dop_from:dop_to]
    
    return (url,title,dop[1:5]+'-'+dop[5:7]+'-'+dop[7:9])


if __name__=="__main__":
    if len(sys.argv)!=5:
        print("Usage: spark-submit news_count_2014.py <input-file-name> <starting date yyyymmdd> <ending date yyyymmdd> <output-file>",file=sys.stderr)
        exit(-1)
    
    """
    Reading in the data using RDD
    """
    #Creating the spark session
    spark = SparkSession \
    .builder \
    .appName("Counting_Articles") \
    .getOrCreate()

    #Creating the spark context
    sc = spark.sparkContext
    
    #Reading in the data file
    total_news_2014 = sc.textFile(sys.argv[1])
    
    #unicode to ascii
    total_news_2014_ascii = total_news_2014.map(lambda x:x.encode("ascii","ignore"))
    

    #Changing the string formatted articles to dictionary
    news_2014_url_title = total_news_2014_ascii.map(lambda x:urlAndTitle(x))

    #Creating the schema
    schema = StructType([
        StructField('Url',StringType(),True),
        StructField('Title',StringType(),True),
        StructField('Dop',StringType(),True),
    ])

    # Using the rdd and schema to convert it into df
    news_url_title_df = spark.createDataFrame(news_2014_url_title,schema)

    #Creating a temporary view using the dataframe
    news_url_title_df.createOrReplaceTempView("news_2014_url_title")
    
    #Creating a new column with date datatype
    news_date_df = news_url_title_df.select("Url","Title","Dop",to_date("Dop",'yyyy-MM-dd').alias('date'))
    
    #Checking for the dates
    staringdate = sys.argv[2]
    endingdate = sys.argv[3]

    if len(staringdate)!=8 or len(endingdate)!=8:
        print("Error: Please make sure starting date and ending date are in format yyyyMMdd",file=sys.stderr)
        exit(-1)
    
    try:
        if int(staringdate)>int(endingdate):
            temp=staringdate
            staringdate = endingdate
            endingdate = temp
    except:
        print("Error: Please make sure the starting dates and ending dates has only numbers",file=sys.stderr)
        exit(-1)

    try:
        datetime(int(staringdate[0:4]),int(staringdate[4:6]),int(staringdate[6:8]))
        datetime(int(endingdate[0:4]),int(endingdate[4:6]),int(endingdate[6:8]))
    except:
        print("Error: Please make sure the month and days are valid",file=sys.stderr)
        exit(-1)

    #Changing the dates yyyyMMdd into yyyy-MM-dd format
    staringdate_new = staringdate[0:4]+'-'+staringdate[4:6]+'-'+staringdate[6:8]
    endingdate_new = endingdate[0:4]+'-'+endingdate[4:6]+'-'+endingdate[6:8]
    
    #Filtering the needed news articles
    desired_dates = news_date_df.filter(news_date_df["date"]>=(lit(staringdate_new)))\
                    .filter(news_date_df["date"]<=(lit(endingdate_new)))
    
    #Counting the articles
    articles_count = desired_dates.select("date").withColumn('COUNT',lit(1))\
                    .groupBy('date').agg(sum("COUNT")).orderBy("date",ascending=True)
    
    articles_count.show(60,False)
    