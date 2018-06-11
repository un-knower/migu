REGISTER /opt/apps/pig-0.12.0-cdh5.11.0/lib/elasticsearch-hadoop-5.4.0.jar
DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.mapping.pig.tuple.use.field.names=true','es.index.auto.create = true','es.nodes=10.0.30.101,10.0.30.102,10.0.30.105,10.0.30.107','es.port=9200');

dns_cache_tmp = LOAD '/tmp/test' USING PigStorage('|') AS (a:chararray,b:chararray,c:chararray,d:chararray);


b= FOREACH dns_cache_tmp GENERATE  a,b,c,d;
 -- dump b;
STORE b INTO 'spark_pig/test' USING EsStorage();