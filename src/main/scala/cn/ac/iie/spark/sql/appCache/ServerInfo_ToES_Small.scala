package cn.ac.iie.spark.sql.appCache

import cn.ac.iie.spark.sql.utils.{DateUtil, SessionUtil}
import org.apache.log4j.{Level, Logger}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by Administrator on 2018/4/11.
  */
object ServerInfo_ToES_Small {
  def main(args: Array[String]): Unit = {
    val time=DateUtil.getUpdateTime("")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SessionUtil.createSparkSession("ServerInfo_ToES_Small")
    val date = DateUtil.getYesterdayTime(args(0))
    spark.sql("use ifp_source")

    val df01=spark.sql("select '1' as rowid,cache_eqp_info as server_ip," +
      "all_num,if(status_code like '2%' or status_code like '3%',1,0)as req_ok_num," +
      "if(cache_hit_status like 'hit',1,0) as req_Hit_num," +
      "if(cache_hit_status like 'miss',1,0) as req_miss_num," +
      "if(cache_hit_status like 'denied',1,0) as req_denied_num," +
      "if(cache_hit_status like 'error',1,0) as req_error_num,flowunit," +
      "all_flow,down_speed,back_flow,if(cache_hit_status like 'hit',all_flow,0)as hit_flow," +
      "if(cache_hit_status like 'miss',back_flow,0) as miss_flow," +
      "if(cache_hit_status like 'miss',all_flow,0) as miss1_flow,user_ip," +
      "'"+time+"'as update_datetime,'1d'as business_time_type,ds as business_time " +
      "from tb_dw_cache_small where ds ='"+date+"'")
    df01.createOrReplaceTempView("temp11")
    val df_temp11=spark.sql("select rowid,server_ip,sum(all_num)as all_num," +
      "sum(req_ok_num)as req_ok_num,sum(all_num-req_ok_num)as req_fail_num" +
      ",sum(req_Hit_num)as req_Hit_num,sum(req_miss_num)as req_miss_num," +
      "sum(req_denied_num)as req_denied_num,sum(req_error_num)as req_error_num," +
      "flowunit,sum(all_flow)as all_flow,avg(down_speed)as down_speed,sum(back_flow) as back_flow," +
      "sum(hit_flow)as hit_flow,sum(miss_flow)as miss_flow," +
      "sum(all_flow-hit_flow-miss1_flow) as other_flow,count(distinct(user_ip))as userip_num," +
      "update_datetime,business_time_type,business_time from temp11 " +
      "group by rowid,server_ip,flowunit,update_datetime,business_time_type,business_time")
    df_temp11.createOrReplaceTempView("temp12")
    val df_small=spark.sql("select rowid,server_ip,all_num,req_ok_num,req_fail_num," +
      "(req_ok_num/all_num)as req_okrate,req_Hit_num,req_Miss_num,(req_Hit_num/all_num) as req_hitrate" +
      ",flowUnit,all_flow,down_speed,back_flow" +
      ",hit_flow,miss_flow,other_flow,(hit_flow/all_flow)as flow_hitrate,userip_num," +
      "update_datetime,business_time_type,business_time from temp12")

    EsSparkSQL.saveToEs(df_small,"ifp_cache_"+date+"_small/TB_APP_Cache_ServerInfo_1d")


    spark.stop()
  }
}
