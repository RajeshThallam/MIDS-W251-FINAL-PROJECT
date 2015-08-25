#!/usr/bin/env python
# encoding: utf-8

# this script reads wiki page daily and monthly trend files from the output of wikipagestats.py
# and parses them as JSON dicts to index them into elasticsearch database using python api

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from hashlib import sha1
from datetime import datetime
import re

def main():
    f = open('/root/wrk/wiki/data/es.wiki.page_trends.201501.txt', 'r')

    # ignore 400 cause by IndexAlreadyExistsException when creating an index
    es = Elasticsearch([{'host': 'spark2', 'port': 9200}])
    es_index_type = "page_trends"
    es_wiki_idx_mapping = {
        'page_trends': {
            'properties': {
                'title': {'type': 'string'}, 
                'page_date': {'type': 'date'},
                'page_views': {'type': 'integer'},
                'daily_trend': {'type': 'float'},
                'monthly_trend': {'type': 'float'}
                }
            }
        }
    es.indices.create(index='wiki', body = {'settings': {'mappings': es_wiki_idx_mapping}}, ignore=400)
    #es.indices.create(index='wiki', ignore=400)

    print datetime.now().strftime("%Y-%m-%d %H:%M") + ": reading"

    pages = []
    idx = 0

    for line in f:
        try:
            trends = line.split('|')
            title = trends[0]
        
            idx += 1
            dates = trends[1].split(',')
            pageviews = trends[2].split(',')
            daily_trends = trends[3].split(',')
            weekly_trends = trends[4].split(',')
            monthly_trends = trends[5].split(',')
            
            sz_mnth_trends = len(monthly_trends)
        
            trends = []
            for i in range(len(dates)):
               try:
                    newpage = {
                        '_index': "wiki",
                        '_type': es_index_type,
                        '_id': sha1(title+dates[i]).hexdigest(),
                        '_source': {
                            'title': title.decode('latin1'),
                            'page_date': datetime.strptime(dates[i], '%Y%m%d'),
                            'page_views': int(pageviews[i]),
                            'daily_trend': float(daily_trends[i]),
                            'monthly_trend': float(monthly_trends[i]) if i < sz_mnth_trends else 0.0
                            }
                        }
                    pages.append(newpage)
               except Exception, e:
                    print line
                    continue
        
            if ( idx % 1000 == 0 ):
                print datetime.now().strftime("%Y-%m-%d %H:%M") + ": indexed wiki[" + es_index_type + "] = " + str(idx)
                helpers.bulk(es, pages, True)
                pages = []
        except Exception, e:
            print line
            continue  

    print datetime.now().strftime("%Y-%m-%d %H:%M") + ": indexed wiki[" + es_index_type + "] = " + str(idx)
    helpers.bulk(es, pages, True)

if __name__ == '__main__':
    main()