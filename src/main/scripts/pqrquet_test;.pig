REGISTER  /opt/apps/pig-0.12.0-cdh5.11.0/lib/libthrift-0.9.3.jar

IMPORT org.apache.pig.builtin
SET parquet.compression gzip;
cache_load = LOAD '/user/hadoop-user/coll_data/cache_log/jiangxi/791/20170805/small/' using PigStorage('|') as (log_record_time:chararray, cache_eqp_info:chararray, user_ip:chararray, user_requst_ip:chararray, http_method:chararray, http_version:chararray, http_host:chararray, http_uri:chararray, http_useragent:chararray, http_referer:chararray, content_type:chararray, status_code:chararray, cache_hit_status:chararray, cache_server_port:chararray, cache_spit_flow:chararray, requst_begintime:chararray, requst_endtime:chararray, fst_response_time:chararray, url_hash:chararray, cache_hit_type:chararray, back_sourceip:chararray, back_targetip:chararray, back_httpmethod:chararray, back_httpversion:chararray, back_httphost:chararray, back_httpuri:chararray, back_httptype:chararray, back_httpcode:chararray, back_closestatus:chararray, back_cachecontrol:chararray, back_maxage:chararray, back_filesize:chararray, cache_savestatus:chararray, back_serverport:chararray, back_flow:long, back_req_begintime:chararray, back_req_endtime:chararray, station_fst_responsetime:chararray);



result= FOREACH cache_load GENERATE  log_record_time,cache_eqp_info,user_ip,user_requst_ip,http_method,http_version,http_host,http_uri,http_useragent,http_referer,content_type,status_code,cache_hit_status,cache_server_port,cache_spit_flow,requst_begintime,requst_endtime,fst_response_time,url_hash,cache_hit_type,back_sourceip,back_targetip,back_httpmethod,back_httpversion,back_httphost,back_httpuri,back_httptype,back_httpcode,back_closestatus,back_cachecontrol,back_maxage,back_filesize,cache_savestatus,back_serverport,back_flow,back_req_begintime,back_req_endtime,station_fst_responsetime;




rmf /ifp_caches/DataCenter/sa/TB_R_W_CACHE/20171127/data/store;
STORE result INTO '/ifp_caches/DataCenter/sa/TB_R_W_CACHE/20171127/data/store'  USING ParquetStorer('\u001');
('\\u001');


-- LOAD DATA INPATH '/ifp_caches/DataCenter/sa/TB_R_W_CACHE/20171127/data/store' OVERWRITE INTO TABLE ${Store_Version}.dm_hn_basic_resource PARTITION (ds_dnslog='${DPI_BATCH_DATE}');
LOAD DATA INPATH '/ifp_caches/DataCenter/sa/TB_R_W_CACHE/20171127/data/store' OVERWRITE INTO TABLE ifp_caches.TB_R_W_CACHE  PARTITION (ds='20170805',prov='330000',type='big');



presto.jar --server 10.0.30.101:8808 --catalog hive --schema ifp_caches

A = load '/user/hadoop-user/coll_data/cache_log/jiangxi/791/20170805/small' USING parquet.pig.ParquetLoader();
