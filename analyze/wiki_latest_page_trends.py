# this script calculates hourly wiki page view trends from raw wiki page view counts
# to run:
#   $ $SPARK_HOME/bin/spark-submit --master local[*] wikipageviewstats.py

# ==============================================================================
# import python libraries
# ==============================================================================
import gzip
import glob
import os
import sys
import re
import urllib

# import spark specific libraries
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark import SparkConf

# import specific libraries
from snakebite.client import Client
from datetime import datetime
from datetime import timedelta
from util.config import Config
import swiftclient as swift

# ==============================================================================
# configuration
# ==============================================================================
cfg = Config()
run_mode = cfg.get("run", "analyze_trends_run_mode")
out_mode = cfg.get("run", "analyze_trends_out_mode")

if (run_mode == "hdfs"):
  source_dir = cfg.get("hdfs", "hdfs_dir_pagecounts")
  hdfs_url = cfg.get("hdfs", "hdfs_url")
  source_files = cfg.get("hdfs", "hdfs_file_pagecounts")
  
elif (run_mode == "swift"):
  source_dir = cfg.get("swift", "swift_container_pagecounts")
  source_files = cfg.get("swift", "swift_file_pagecounts")

  swift_authurl = cfg.get("swift", "st_auth")
  swift_user = cfg.get("swift", "st_user")
  swift_key = cfg.get("swift", "st_key")
  swift_region = cfg.get("swift", "region")

  SWIFT_DEFAULT_CONFIGS = {
    "fs.swift.service." + swift_region + ".apikey": swift_key,
    "fs.swift.service." + swift_region + ".username": swift_user
  }

else:
  source_dir = cfg.get("local", "local_dir_pagecounts")
  source_files = cfg.get("local", "local_file_pagecounts")

if (out_mode == "hdfs"):
  target_dir = cfg.get("hdfs", "hdfs_dir_latest_trends")
elif (out_mode == "swift"):
  target_container = cfg.get("swift", "swift_container_latest_trends")
  target_dir = "swift://" + target_container + "." + swift_region + "/"
else:
  target_dir = cfg.get("local", "local_dir_trends")

# ==============================================================================
# define regex for cleansing titles
# ==============================================================================
# Exclude pages other than english language
wiki_regex = re.compile('en (.*) ([0-9].*) ([0-9].*)')
first_letter_is_lower_regex = re.compile('([a-z])(.*)')
img_regex = re.compile('(.*).(jpg|gif|png|JPG|GIF|PNG|txt|ico)')
blacklist = [
    '404_error/',
    'Main_Page',
    'Hypertext_Transfer_Protocol',
    'Favicon.ico',
    'Search'
    ]

# Excludes pages outside of namespace 0 (ns0)
namespace_titles_regex = re.compile(
    '(Media|Special' + 
    '|Talk|User|User_talk|Project|Project_talk|File' +
    '|File_talk|MediaWiki|MediaWiki_talk|Template' +
    '|Template_talk|Help|Help_talk|Category' +
    '|Category_talk|Portal|Wikipedia|Wikipedia_talk)\:(.*)')

# ==============================================================================
# local functions
# ==============================================================================
def clean_redirects(page):
  """
  pages like Facebook#Website really are "Facebook",
  ignore/strip anything starting at # from pagename
  """ 
  anchor = page.find('#')
  if anchor > -1:
    page = page[0:anchor]
  return page  

def is_valid_title(title):
  is_outside_namespace_zero = namespace_titles_regex.match(title)
  if is_outside_namespace_zero is not None:
    return False
  islowercase = first_letter_is_lower_regex.match(title)
  if islowercase is not None:
    return False
  is_image_file = img_regex.match(title)
  if is_image_file:
    return False  
  has_spaces = title.find(' ')
  if has_spaces > -1:
    return False
  if title in blacklist:
    return False   
  return True  

def parse_in_data(line, date):
  d = []
  l = wiki_regex.match(line)
  title = ""
  if l is not None:
    page, count, bytes = l.groups()
    if is_valid_title(page):
      title = clean_redirects(urllib.unquote_plus(page))
      if len(title) > 0 and title[0] != '#':
        return (( (title, date), int(count)))

def daily_trend(pageviews):
  curr_views = iter(pageviews)
  prev = next(curr_views)
  for curr in curr_views:
    yield (1.0*curr/prev) - 1
    prev = curr

def weekly_trend(pageviews):
  curr_views = pageviews
  idx = 0
  prev = curr_views[idx]
  for curr in curr_views:
    if idx > 13: 
      prev = sum(curr_views[idx-14:idx-7])
      curr = sum(curr_views[idx-7:idx])
    else:
      prev = 0
      curr = 0
    
    yield (1.0*curr/prev) - 1 if idx > 13 else 0.0 
    idx +=1

def monthly_trend(pageviews):
  curr_views = pageviews
  idx = 0
  prev = curr_views[idx]
  for curr in curr_views:
    if idx > 59: 
      prev = sum(curr_views[idx-60:idx-30])
      curr = sum(curr_views[idx-30:idx])
    else:
      prev = 0
      curr = 0

    yield (1.0*curr/prev) - 1 if idx > 59 else 0.0 
    idx +=1

def calc_trend(page, dates, pageviews):
  dts, counts = zip( *sorted( zip (dates, pageviews)))

  # daily trend
  daily_trends = [0.0]
  if len(counts) > 1:
    daily_trends.extend([x for x in daily_trend(counts)])
  else:
    daily_trends = [0.0]

  # weekly trend
  weekly_trends = [0.0]
  if len(counts) > 13:
    weekly_trends.extend([x for x in weekly_trend(counts)])
  else:
    weekly_trends = [0.0]

  # monthly trend
  monthly_trends = [0.0]
  if len(counts) > 13:
    monthly_trends.extend([x for x in monthly_trend(counts)])
  else:
    monthly_trends = [0.0]

  # prepare for return strings
  date_str = '%s|' % ','.join(dts)
  pageview_str = '%s|' % ','.join(map(str,counts))
  daily_trend_str = '%s|' % ','.join(map(str,daily_trends))
  weekly_trend_str = '%s|' % ','.join(map(str,weekly_trends))
  monthly_trend_str = '%s|' % ','.join(map(str,monthly_trends))

  # pipe '|' is forbidden in wiki titles and would make a good delimiter
  out_str = page.strip() + "|" + date_str + pageview_str + daily_trend_str + weekly_trend_str + monthly_trend_str
  return out_str


# define spark context
conf = (SparkConf()
        .setAppName("Wiki Page Views Trends")
        .set("spark.hadoop.validateOutputSpecs", "false"))
sc  = SparkContext(conf = conf)

# set custom connection
if (run_mode == "hdfs" or out_mode == "hdfs"):
  # spotify's snakebite as hdfs client
  hdfs_client = Client("spark1", 9000, use_trash=False)

if (run_mode == "swift" or out_mode == "swift"):
  swiftConf = sc._jsc.hadoopConfiguration()
  for key, value in SWIFT_DEFAULT_CONFIGS.items():
    swiftConf.set(key, value)

  swift_client = swift.Connection(
    user = swift_user, 
    key = swift_key, 
    authurl = swift_authurl)

# read list of files
src_files = []

if run_mode == "hdfs":
  # spotify's snakebite as hdfs client
  src_files = [ hdfs_url + files['path'] for files in hdfs_client.ls([source_files]) ]

  # deleting output directory if exists
  if (hdfs_client.test(target_dir, exists = True, directory = True)):
    hdfs_client.delete(target_dir)
    hdfs_client.rmdir(target_dir)

elif run_mode == "swift":  
  # read list of files from swift  src_files = []
  source_files = '|'.join([ '(pagecounts-' + (datetime.now() - timedelta(hours=i)).strftime("%Y%m%d-%H") + '(.*))' for i in range(48, 71) ])
  src_file_regex = re.compile(source_files)
  for data in swift_client.get_container(source_dir)[1]:
     if src_file_regex.match(data['name']):
       src_files.append(data['name'])
  
  src_files.sort(key = lambda x: os.path.basename(x))

else:
  # read list of files from local
  src_files = filter(os.path.isfile, glob.glob(os.path.join(source_dir, source_files)))
  src_files.sort(key = lambda x: os.path.basename(x))

page_w_date = sc.parallelize([])
rdds = []

# read wiki page count stats and clean wiki page titles 
for src_file_name in src_files:
  base = os.path.basename(src_file_name)
  filename_tokens = base.split('-')
  (date, time) = filename_tokens[1], filename_tokens[2].split('.')[0] 
    
  if run_mode == "swift":
    src_file_name = "swift://" + source_dir + "." + swift_region + "/" + src_file_name
    
  lines = sc.textFile(src_file_name)
  parts = lines\
    .filter(lambda l: wiki_regex.match(l)) \
    .filter(lambda line: "facebook" in line.lower() ) \
    .map(lambda l: parse_in_data(l, date + time)) \
    .filter(lambda l: l != None)
#   .filter(lambda line: "facebook" in line.lower() ) \

  rdds.append(parts)

page_w_date = sc.union(rdds)

# calculate trends
pageview_counts = page_w_date \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda ( (p, d), c): (p, ([ d ], [ c ])) ) \
    .reduceByKey(lambda (d0, c0), (d1, c1): (d0 + d1, c0 + c1) ) \
    .map(lambda ( p, (d, c)): calc_trend(p, d, c) )

# write output to target directory
pageview_counts.saveAsTextFile(target_dir)