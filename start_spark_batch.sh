#!/bin/bash

log_file=/root/wrk/wiki/log/wiki_pageview_trends.log.$(date +%Y%m%d.%H%M)

nohup $SPARK_HOME/bin/spark-submit --executor-memory 4G --driver-memory 4G --driver-cores 2 --executor-cores 2 --num-executors 4 --master spark://spark01:7077 --jars /usr/loc
al/hadoop/share/hadoop/tools/lib/hadoop-openstack-2.6.0.jar wiki_pageview_trends.py > ${log_file} 2>&1 &

tail -f ${log_file}