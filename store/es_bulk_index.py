
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