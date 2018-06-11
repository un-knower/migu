


drop table if exists ifp_caches.TB_R_W_CACHE;
CREATE TABLE IF NOT EXISTS ifp_caches.TB_R_W_CACHE 
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
) PARTITIONED BY (ds string,prov string,type string)
 row format delimited fields terminated by '\001'
stored as parquet;
drop table if exists ifp_caches.TB_R_W_CACHE_P;
CREATE external  TABLE IF NOT EXISTS ifp_caches.TB_R_W_CACHE_p 
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

alter table ifp_caches.TB_R_W_CACHE_p  add if not exists PARTITION(ds='20170805',prov='330000',type='small') location '/ifp_caches/DataCenter/sa/TB_R_W_CACHE/2017112915/data/store'; 


use ifp_caches;
alter table TB_R_W_CACHE_P add if not exists PARTITION(ds='20170805',prov='330000',type='big') location '/ifp_source/DataCenter/sa/TB_R_W_CACHE/20170805/big/2017120415/data/store'; 
alter table TB_R_W_CACHE_P add if not exists PARTITION(ds='20170805',prov='330000',type='small') location '/ifp_source/DataCenter/sa/TB_R_W_CACHE/20170805/small/2017120415/data/store'; 

alter table TB_R_W_CACHE_P add if not exists PARTITION(ds='20170806',prov='330000',type='big') location '/ifp_source/DataCenter/sa/TB_R_W_CACHE/20170806/big/2017120415/data/store';  
alter table TB_R_W_CACHE_P add if not exists PARTITION(ds='20170806',prov='330000',type='small') location '/ifp_source/DataCenter/sa/TB_R_W_CACHE/20170806/small/2017120415/data/store';  


insert into table ifp_caches.TB_R_W_CACHE_P  PARTITION (ds='20170805',prov='330000',type='big')  select date,deviceip,userip,serverip,httpmethod,httpversion,host,uri,useragent,referer,contenttype,statuscode,cachestate,cachehost,cacheflow,requeststartdate,requestenddate,firstresponsetime,urlhashcode,cachetype,backsourceip,backtargetip,backhttpmethod,backhttpversion,backhost,backuri,backcontenttype,backstate,backclosestate,backcachecontrol,backmaxgge,backfilesize,backsavestate,backserverport,backflow,backrequeststartdate,backrequestenddate,backfirstresponsetime from ifp_caches.TB_R_W_CACHE where ds='20170805' and prov='330000' and type='big';  
LOAD DATA INPATH '/user/hadoop-user/coll_data/cache_log/jiangxi/791/ceshi/20170805/big' OVERWRITE INTO TABLE ifp_caches.TB_R_W_CACHE  PARTITION (ds='20170805',prov='330000',type='big');
LOAD DATA INPATH '/user/hadoop-user/coll_data/cache_log/jiangxi/791/ceshi/20170805/small' OVERWRITE INTO TABLE ifp_caches.TB_R_W_CACHE  PARTITION (ds='20170805',prov='330000',type='small');

LOAD DATA INPATH '/user/hadoop-user/coll_data/cache_log/jiangxi/791/ceshi/20170806/big' OVERWRITE INTO TABLE ifp_caches.TB_R_W_CACHE_p  PARTITION (ds='20170806',prov='330000',type='big');
LOAD DATA INPATH '/user/hadoop-user/coll_data/cache_log/jiangxi/791/20170806/small' OVERWRITE INTO TABLE ifp_caches.TB_R_W_CACHE_p  PARTITION (ds='20170806',prov='330000',type='small');
 hdfs://node01:8020/user/hive/warehouse/ifp_caches.db/tb_r_w_cache_p/ds=20170806/prov=330000/type=small/