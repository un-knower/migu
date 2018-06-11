use ${Store_Version};
alter table TB_R_W_CACHE add if not exists PARTITION(ds='${DNS_LOG_datatime}',prov='${DNS_LOG_code}',type='${file_type}') location '${DNS_LOG_datapath}';

alter table TB_R_W_DNS_LOG add if not exists PARTITION(ds='${DNS_LOG_datatime}',prov='${DNS_LOG_code}') location '${DNS_LOG_datapath}';





/*/user/hadoop-user/coll_data/cache_log/jiangxi/791/20170805/big
/user/hadoop-user/coll_data/cache_log/jiangxi/791/20170805/small*/
use ifp_source;
alter table TB_R_W_CACHE add if not exists PARTITION(ds='20170805',prov='330000',type='big') location '/ifp_source/DataCenter/sa/TB_R_W_CACHE/20170805/big/2017120415/data/store'; 
alter table TB_R_W_CACHE add if not exists PARTITION(ds='20170805',prov='330000',type='small') location '/ifp_source/DataCenter/sa/TB_R_W_CACHE/20170805/small/2017120415/data/store'; 

alter table TB_R_W_CACHE add if not exists PARTITION(ds='20170806',prov='330000',type='big') location '/ifp_source/DataCenter/sa/TB_R_W_CACHE/20170806/big/2017120415/data/store';  
alter table TB_R_W_CACHE add if not exists PARTITION(ds='20170806',prov='330000',type='small') location '/ifp_source/DataCenter/sa/TB_R_W_CACHE/20170806/small/2017120415/data/store';  

use ifp_source;
alter table TB_R_W_DNS_LOG add if not exists PARTITION(ds='20171025',prov='330000') location '/ifp_source/DataCenter/sa/TB_R_W_DNS_LOG/20171025/2017120415/data/store';
alter table TB_R_W_DNS_LOG add if not exists PARTITION(ds='20171026',prov='330000') location '/ifp_source/DataCenter/sa/TB_R_W_DNS_LOG/20171026/2017120415/data/store';


-- 20170805/big small
nohup /opt/apps/spark/bin/spark-submit  --class  cn.ac.iie.spark.streaming.scala.sql.FormatConversion    --master spark://10.0.30.101:7077     --driver-memory 10G    --executor-memory 39G    --executor-cores 4    --num-executors 9    --conf "spark.ui.showConsoleProgress=false"  --conf "spark.eventLog.enabled=true"  --conf "spark.eventLog.dir=hdfs://10.0.30.101:8020/sparkhistory"  --conf "spark.driver.extraJavaOptions=-XX:MaxPermSize=6G -XX:+UseConcMarkSweepGC"    --conf "spark.sql.tungsten.enabled=false"    --conf "spark.sql.codegen=false"    --conf "spark.sql.unsafe.enabled=false"    --conf "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC -Dlog4j.configuration=log4j-eir.properties"    --conf "spark.streaming.backpressure.enabled=true"    --conf "spark.locality.wait=1s"    --conf "spark.streaming.blockInterval=1500ms"    --conf "spark.shuffle.consolidateFiles=true"     /home/hadoop-user/zm/Hours-0.1.jar >dataProcess_cache.log &

--20171025 dns
nohup /opt/apps/spark/bin/spark-submit  --class  cn.ac.iie.spark.streaming.scala.sql.DnsConvert  --master spark://10.0.30.101:7077   --executor-memory 39G    --executor-cores 4    --num-executors 9    --conf "spark.ui.showConsoleProgress=false"  --conf "spark.eventLog.enabled=true"  --conf "spark.eventLog.dir=hdfs://10.0.30.101:8020/sparkhistory"  --conf "spark.driver.extraJavaOptions=-XX:MaxPermSize=6G -XX:+UseConcMarkSweepGC"    --conf "spark.sql.tungsten.enabled=false"    --conf "spark.sql.codegen=false"    --conf "spark.sql.unsafe.enabled=false"    --conf "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC -Dlog4j.configuration=log4j-eir.properties"    --conf "spark.streaming.backpressure.enabled=true"    --conf "spark.locality.wait=1s"    --conf "spark.streaming.blockInterval=1500ms"    --conf "spark.shuffle.consolidateFiles=true"     /home/hadoop-user/zm/Hours-0.1.jar >dataProcess.log &


nohup /opt/apps/spark/bin/spark-submit  --class  cn.ac.iie.spark.streaming.scala.sql.DnsConvert  hdfs://10.0.30.101:8020/user/hadoop-user/coll_data/dns_log/jiangxi/791/20171026  dns  hdfs://10.0.30.101:8020/ifp_source/DataCenter/sa/TB_R_W_DNS_LOG/20171026/2017120415/data/store --master spark://10.0.30.101:7077       --driver-memory 10G    --executor-memory 39G    --executor-cores 4    --num-executors 9    --conf "spark.ui.showConsoleProgress=false"  --conf "spark.eventLog.enabled=true"  --conf "spark.eventLog.dir=hdfs://10.0.30.101:8020/sparkhistory"  --conf "spark.driver.extraJavaOptions=-XX:MaxPermSize=6G -XX:+UseConcMarkSweepGC"    --conf "spark.sql.tungsten.enabled=false"    --conf "spark.sql.codegen=false"    --conf "spark.sql.unsafe.enabled=false"    --conf "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC -Dlog4j.configuration=log4j-eir.properties"    --conf "spark.streaming.backpressure.enabled=true"    --conf "spark.locality.wait=1s"    --conf "spark.streaming.blockInterval=1500ms"    --conf "spark.shuffle.consolidateFiles=true"     /home/hadoop-user/zm/Hours-0.1.jar >dataProcess.log &





hdfs://10.0.30.101:8020/user/hadoop-user/coll_data/dns_log/jiangxi/791/20171025
dns
hdfs://10.0.30.101:8020/ifp_caches/DataCenter/sa/TB_R_W_DNS/2017120414/data/store



./presto --server 10.0.30.101:8808 --catalog hive --schema ifp_source


presto.jar --server 10.0.30.101:8808 --catalog hive --schema ifp_source




TB_R_W_CACHE
