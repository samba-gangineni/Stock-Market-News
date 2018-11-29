from __future__ import print_function
from datetime import datetime
import sys
import pandas as pd
from plotly.offline import plot,iplot
import plotly.graph_objs as go
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit, to_date, sum


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

'''
    This function returns the html
'''
def image_to_html(division_data):
    return """
    <!DOCTYPE html>
    <html>
      <head>
        <title>News Article Count</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
      </head>
      <body>
        <div>
          {}
        </div>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
      </body>
    </html>
    """.format(division_data)

if __name__=="__main__":
    if len(sys.argv)!=5:
        print("Usage: spark-submit news_count_2014.py <input-file-name> <starting date yyyymmdd> <ending date yyyymmdd> <output-file>",file=sys.stderr)
        exit(-1)
    
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
    
    #Checking for extensions
    if '.' in sys.argv[4]:
        print("Error: make sure that you provide plain output file name",file=sys.stderr)
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
    
    #Changing the dates yyyyMMdd into yyyy-MM-dd format
    staringdate_new = staringdate[0:4]+'-'+staringdate[4:6]+'-'+staringdate[6:8]
    endingdate_new = endingdate[0:4]+'-'+endingdate[4:6]+'-'+endingdate[6:8]
    
    #Filtering the needed news articles
    desired_dates = news_date_df.filter(news_date_df["date"]>=(lit(staringdate_new)))\
                    .filter(news_date_df["date"]<=(lit(endingdate_new)))
    
    #Counting the articles
    articles_count = desired_dates.select("date").withColumn('COUNT',lit(1))\
                    .groupBy('date').agg(sum("COUNT")).orderBy("date",ascending=True)
    
    #Converting this dataframe into pandas dataframe for plotting
    articles_count_pandas = articles_count.toPandas()
    articles_count_pandas["date"] = articles_count_pandas["date"].astype('datetime64[ns]')
    
    #Creating the graph with the range slider
    # first providing the data for the line
    trace0 = go.Scatter(
        x = articles_count_pandas["date"],
        y = articles_count_pandas["sum(COUNT)"],
        name = "Articles_Count",
        line = dict(color = '#17BECF'),
        opacity=0.8)
    
    data = [trace0]

    #Providing the layout info
    layout = dict(
        title='Timeseries of news articles count from {} to {}'.format(staringdate_new,endingdate_new),
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                        label='1m',
                        step='month',
                        stepmode='backward'),
                    dict(count=6,
                        label='6m',
                        step='month',
                        stepmode='backward'),
                    dict(step='all')
                ])
            ),
            rangeslider = dict(
                visible=True
            ),
            type='date'

        )
        
    )

    #data and layout for the figure
    fig = dict(data=data,layout=layout)

    #html division tag is the output
    html_division = plot(fig,output_type='div')
    
    # Inserting the chart into a division
    html_script = image_to_html(html_division)

    with open(sys.argv[4]+'.html','w') as imgToHtml:
        imgToHtml.write(html_script)
    
    sc.stop()
    