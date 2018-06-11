drop database IF EXISTS ifp_source cascade;
CREATE DATABASE IF NOT EXISTS ifp_source COMMENT 'ifp_source DATABASE v1.0' WITH DBPROPERTIES ('creator'='Beijing Neteast Technologies Co.Ltd','createTime'='2018-01-17');

--______________________________________________________________zhgm

--drop table if exists ifp_source.TB_R_W_CACHE;
--CREATE external  TABLE IF NOT EXISTS ifp_source.TB_R_W_CACHE
--(
--    date                    string,
--    deviceip                string,
--    userip                  string,
--    serverip                string,
--    httpmethod              string,
--    httpversion             string,
--    host                    string,
--    uri                     string,
--    useragent               string,
--    referer                 string,
--    contenttype             string,
--    statuscode              string,
--    cachestate              string,
--    cachehost               string,
--    cacheflow               string,
--    requeststartdate        string,
--    requestenddate          string,
--    firstresponsetime       string,
--    urlhashcode             string,
--    cachetype               string,
--    backsourceip            string,
--    backtargetip            string,
--    backhttpmethod          string,
--    backhttpversion         string,
--    backhost                string,
--    backuri                 string,
--    backcontenttype         string,
--    backstate               string,
--    backclosestate          string,
--    backcachecontrol        string,
--    backmaxgge              string,
--    backfilesize            string,
--    backsavestate           string,
--    backserverport          string,
--    backflow                string,
--    backrequeststartdate    string,
--    backrequestenddate      string,
--    backfirstresponsetime   string
--) PARTITIONED BY (ds string,prov string,type string)
--row format delimited fields terminated by '\001' STORED as PARQUET;

drop  table if exists ifp_source.TB_R_W_DNS_LOG;
CREATE external TABLE IF NOT EXISTS ifp_source.TB_R_W_DNS_LOG
(
  ip_source       string,
  domain  string,
  dd_time_collect string,
  ip_target       string,
  dns_parse_type  int
)PARTITIONED BY (ds string,prov string)
row format delimited fields terminated by '\001' STORED as PARQUET;

--______________________________________________________________
--______________________________________________________________xuxf
use ifp_source;


--______________________________________________________________TB_SA_Cache_small
drop table TB_SA_Cache_small;
create table TB_SA_Cache_small(
  `date`    string,
  deviceip    string,
  userip    string,
  serverip    string,
  httpmethod    string,
  httpversion    string,
  host    string,
  uri    string,
  useragent    string,
  referer    string,
  contenttype    string,
  statuscode    string,
  cachestate    string,
  cachehost    string,
  cacheflow    string,
  requeststartdate    string,
  requestenddate    string,
  firstresponsetime    string,
  urlhashcode    string,
  cachetype    string,
  backsourceip    string,
  backtargetip    string,
  backhttpmethod    string,
  backhttpversion    string,
  backhost    string,
  backuri    string,
  backcontenttype    string,
  backstate    string,
  backclosestate    string,
  backcachecontrol    string,
  backmaxgge    string,
  backfilesize    string,
  backsavestate    string,
  backserverport    string,
  backflow    string,
  backrequeststartdate    string,
  backrequestenddate    string,
  backfirstresponsetime    string,
  factory    string,
  cache_type    string,
  city    string,
  access_type    string,
  user_terminal    string,
  websiteid    string,
  websitename    string,
  main_domain    string,
  file_type    string,
  province    string
)
PARTITIONED BY (
  ds    string,
  prov    string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='/001', 
  'serialization.format'='/001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ("orc.compress"="SNAPPY");


--___________________________________________________TB_SA_Cache_big
drop table TB_SA_Cache_big;
create table TB_SA_Cache_big(
  `date`    string,
  deviceip    string,
  userip    string,
  serverip    string,
  httpmethod    string,
  httpversion    string,
  host    string,
  uri    string,
  useragent    string,
  referer    string,
  contenttype    string,
  statuscode    string,
  cachestate    string,
  cachehost    string,
  cacheflow    string,
  requeststartdate    string,
  requestenddate    string,
  firstresponsetime    string,
  urlhashcode    string,
  cachetype    string,
  backsourceip    string,
  backtargetip    string,
  backhttpmethod    string,
  backhttpversion    string,
  backhost    string,
  backuri    string,
  backcontenttype    string,
  backstate    string,
  backclosestate    string,
  backcachecontrol    string,
  backmaxgge    string,
  backfilesize    string,
  backsavestate    string,
  backserverport    string,
  backflow    string,
  backrequeststartdate    string,
  backrequestenddate    string,
  backfirstresponsetime    string,
  factory    string,
  cache_type    string,
  city    string,
  access_type    string,
  user_terminal    string,
  websiteid    string,
  websitename    string,
  main_domain    string,
  file_type    string,
  province    string,
  chanel_id    string,
  chanel_name    string,
  uri_prefix    string
)PARTITIONED BY (
  ds    string,
  prov    string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
  'field.delim'='/001', 
  'serialization.format'='/001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ("orc.compress"="SNAPPY");



--___________________________________________________TB_DW_Cache_small
drop table TB_DW_Cache_small;
create table TB_DW_Cache_small(
factory    string,
user_ip    string,
cache_type    string,
cache_eqp_info    string,
city    string,
access_type    string,
user_terminal    string,
file_type    string,
webSiteID    string,
webSiteName    string,
domain    string,
main_domain    string,
cache_hit_status    string,
status_code    string,
back_status_code    string,
http_method    string,
back_closestatus    string,
backcachecontrol    string,
backmaxgge    string,
all_num    bigint,
flowunit    string,
all_flow    double,
down_speed    double,
fstpack_delay    bigint,
back_fstpack_delay    bigint,
back_flow    double,
back_downspeed    double,
province    string,
update_datetime    string,
business_time    string,
business_hour    string
)
PARTITIONED BY ( 
  ds string,
  prov string)
ROW format delimited fields terminated BY '\001';


--___________________________________________________TB_DW_Cache_big
drop table TB_DW_Cache_big;
create table TB_DW_Cache_big(
factory    string,
user_ip    string,
cache_type    string,
cache_eqp_info    string,
city    string,
access_type    string,
user_terminal    string,
file_type    string,
webSiteID    string,
webSiteName    string,
domain    string,
main_domain    string,
cache_hit_status    string,
status_code    string,
back_status_code    string,
http_method    string,
back_closestatus    string,
backcachecontrol    string,
backmaxgge    string,
all_num    bigint,
flowunit    string,
all_flow    double,
down_speed    double,
fstpack_delay    bigint,
back_fstpack_delay    bigint,
back_flow    double,
back_downspeed    double,
province    string,
update_datetime    string,
business_time    string,
business_hour    string,
chanel_id    string,
chanel_name    string,
uri_prefix    string
)
PARTITIONED BY ( 
  ds string,
  prov string)
ROW format delimited fields terminated BY '\001';

--___________________________________________________TB_DW_Cache_big_url
drop table TB_DW_Cache_big_url;
create table TB_DW_Cache_big_url(
factory    string,
webSiteID    string,
webSiteName    string,
chanel_id    string,
chanel_name    string,
uri_prefix    string,
file_type    string,
uri    string,
user_ip    string,
cache_hit_status    string,
status_code    string,
back_status_code    string,
http_method    string,
back_closestatus    string,
backcachecontrol    string,
all_num    bigint,
flowunit    string,
all_flow    double,
back_flow    double,
province    string,
update_datetime    string,
business_time    string)
PARTITIONED BY ( 
  ds string,
  prov string)
ROW format delimited fields terminated BY '\001';

--________________________________________________________________________
--__________________________________APP___________________________________
--______________________TB_APP_Cache_Overview_____________________________



