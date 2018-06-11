package cn.ac.iie.spark.sql.appCache

import cn.ac.iie.spark.sql.utils.{DateUtil, SessionUtil}
import org.apache.log4j.{Level, Logger}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by Administrator on 2018/4/11.
  */
object IpAnalysis_ToES_Big {
  def main(args: Array[String]): Unit = {
    val time=DateUtil.getUpdateTime("")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SessionUtil.createSparkSession("IpAnalysis_ToES")
    val date = DateUtil.getYesterdayTime(args(0))
    spark.sql("use ifp_source")


    val df=spark.sql("select '1' as rowid,factory,websiteid,websitename,cache_type,user_ip,all_num," +
      "if(status_code like '2%' or status_code like '3%',1,0) as req_ok_num," +
      "if(cache_hit_status like 'hit',1,0) as req_Hit_num,if(cache_hit_status like 'miss',1,0) " +
      "as req_miss_num,if(cache_hit_status like 'denied',1,0) as req_denied_num," +
      "if(cache_hit_status like 'error',1,0) as req_error_num,if(Status_code like '2%',1,0)" +
      " as return2xx_num,if(Status_code like '3%',1,0) as return3xx_num,if(Status_code like" +
      " '4%',1,0) as return4xx_num,if(Status_code like '5%',1,0) as return5xx_num,flowunit," +
      "all_flow,if(cache_hit_status like 'hit',all_flow,0) as hit_flow,if(cache_hit_status " +
      "like 'miss',back_flow,0) as miss_flow,if(cache_hit_status like 'miss',all_flow,0) as " +
      "miss1_flow,'"+time+"' as update_datetime,'1d' as business_time_type,ds as business_time " +
      "from tb_dw_cache_big where ds = '"+date+"'")
    df.createOrReplaceTempView("temp01")
    val df_big=spark.sql("select rowid,factory,webSiteID,webSiteName,cache_type,user_ip," +
      "sum(all_num) as all_num,sum(req_ok_num) as req_ok_num,sum(all_num-req_ok_num)" +
      " as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_miss_num) as " +
      "req_Miss_num," +
      "sum(return2xx_num) as return2xx_num,sum(return3xx_num) as " +
      "return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as " +
      "return5xx_num,flowunit,sum(all_flow) as all_flow,sum(hit_flow) as hit_flow," +
      "sum(miss_flow) as miss_flow,sum(all_flow-hit_flow-miss1_flow) as other_flow," +
      "update_datetime,business_time_type,business_time from temp01 group by rowid,factory" +
      ",websiteid,websitename,cache_type,user_ip,flowUnit,update_datetime,business_time_type,business_time")
    EsSparkSQL.saveToEs(df_big,"ifp_cache_"+date+"_big/TB_APP_Cache_ipanalysis_1d")

    spark.stop()
  }
}
