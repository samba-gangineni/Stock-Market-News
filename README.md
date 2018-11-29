#news_count_2014.py

## News Articles Count
News Articles Count is a tool which plots the number of articles for each day.This is not just
limited to the news articles count, this python file can be used to any type of data which has 
time stamp. 

The changes must be made to some parts of the script according to the needs of your data.


## Requirements
1. This program uses the distributed processing capacity of Apache Spark 2.4.0
2. Also, this is built using python, pandas, plotly
3. Install all python dependencies as following. Donot install the dependencies to a virtual 
environemnt.
$ pip install -r requirements.txt

## Usage
user@server.com$ spark-submit news_count_2014.py <inputJsonformatfile> <startdate yyyyMMdd> <enddate yyyyMMdd> <output>

Example: spark-submit news_count_2014.py news.json.bz2 20140101 20141231 index

Here the order of start date and end date is not important.

## Results
This program outputs the html file, and we can view the chart in this html file. The chart consists of
slider, using which we can see the counts of news articles over a period of some days. Once the slider
is adjusted we can save the file by clicking the camera icon in the chart. This saves the image in
png format

