# MIDS W251 FINAL PROJECT
Repo for final project of "MIDS W251 - Scaling up Really Big Data" course

## Wikipedia Trending Pages Analysis

### Team

- Amir Zai
- Rajesh Thallam
- Shelly Stanley

### Abstract
Our project, “Wikipedia Trending Pages Analysis”, is an analytics tool tracking trending pages in Wikipedia. Wikipedia is one of the top most visited websites on the Internet with millions of articles, 18 billion page views and 500 million unique visitors per months. We gathered and analyzed traffic data and developed a system to (i) display the top 10 trending pages on Wikipedia in last 24 hours (ii) display top 10 trending pages on Wikipedia in last 30 days (iii) predicting page view traffic for the trending pages in last 24 hours and (iv) search and view past trends for a page or article based on users interest. The tool is built on Apache Spark platform hosted on HDFS and Hive with the end results visualized on Kibana hosted on ElasticSearch index. The project has successfully demonstrated how to scale up a data pipeline to process, store and analyze batches and streams of large data sets of volumes in terabytes and analyze traffic for approximately 3.5 million articles.

**Keywords**: Wikipedia; Predictions; Trend Analysis; Apache Spark; Apache Hive; Elastic Search; Kibana; SoftLayer. 

### Data Set
https://aws.amazon.com/datasets/4182
http://dumps.wikimedia.org/other/pagecounts-raw/2015/
https://dumps.wikimedia.org/enwiki/latest/