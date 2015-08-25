
es_rdd = sc.newAPIHadoopRDD(
	inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
	keyClass="org.apache.hadoop.io.NullWritable", 
	valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
	conf=es_read_conf)

lines = sc.textFile(filename)
parts = lines.map(lambda x:x.split(' '))
wikidata = parts.map(lambda p: Row(projectcode=p[0], pagename=p[1].lower(), pageviews=int(p[2]), size=int(p[3])))
df = sqlContext.createDataFrame(wikidata)
df_filtered = df.filter((df.projectcode == 'en') & (df.pageviews>MIN_PAGEVIEWS))

df_filtered.rdd.saveAsNewAPIHadoopFile(
	path='-',
	outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
	keyClass="org.apache.hadoop.io.NullWritable",
	valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
	conf=es_write_conf) 
	
	
es_write_conf = {
	"es.nodes" : cfg.get("es", "es_master_node"),
	"es.port" : cfg.get("es", "es_port"),
	"es.resource" : cfg.get("es", "dbname") + "/" + cfg.get("es", "index_type") 
}

es_index_type = "page_trends"
es_wiki_idx_mapping = {
	'page_trends': {
		'properties': {
			'title': {'type': 'string'}, 
			'page_date': {'type': 'date'},
			'page_views': {'type': 'integer'},
			'daily_trend': {'type': 'float'},
			'weekly_trend': {'type': 'float'}
			'monthly_trend': {'type': 'float'}
			}
		}
	}

es_idx.rdd.saveAsNewAPIHadoopFile(
	path='-',
	outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
	keyClass="org.apache.hadoop.io.NullWritable",
	valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
	conf = es_write_conf) 