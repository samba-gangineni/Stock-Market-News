from __future__ import print_function
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit, to_date
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
    
    #Filtering the df based on the input dates
    desired_dates = 
    