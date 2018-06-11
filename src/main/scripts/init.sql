drop database if exists ifp_source  cascade; 

CREATE DATABASE IF NOT EXISTS ifp_source COMMENT 'ifp_source DATABASE v1.0' WITH DBPROPERTIES ('creator'='Beijing Neteast Technologies Co.Ltd','createTime'='2017-11-21');

drop table if exists ifp_source.TB_R_W_CACHE;
CREATE external  TABLE IF NOT EXISTS ifp_source.TB_R_W_CACHE 
(
    date                    string,                                      
    deviceip                string,                                      
    userip                  string,                                      
    serverip                string,                                      
    httpmethod              string,                                      
    httpversion             string,                                      
    host                    string,                                      
    uri                     string,                                      
    useragent               string,                                      
    referer                 string,                                      
    contenttype             string,                                      
    statuscode              string,                                      
    cachestate              string,                                      
    cachehost               string,                                      
    cacheflow               string,                                      
    requeststartdate        string,                                      
    requestenddate          string,                                      
    firstresponsetime       string,                                      
    urlhashcode             string,                                      
    cachetype               string,                                      
    backsourceip            string,                                      
    backtargetip            string,                                      
    backhttpmethod          string,                                      
    backhttpversion         string,                                      
    backhost                string,                                      
    backuri                 string,                                      
    backcontenttype         string,                                      
    backstate               string,                                      
    backclosestate          string,                                      
    backcachecontrol        string,                                      
    backmaxgge              string,                                      
    backfilesize            string,                                      
    backsavestate           string,                                      
    backserverport          string,                                      
    backflow                string,                                      
    backrequeststartdate    string,                                      
    backrequestenddate      string,                                      
    backfirstresponsetime   string
) PARTITIONED BY (ds string,prov string,type string)   row format delimited fields terminated by '\001' STORED as PARQUET;

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