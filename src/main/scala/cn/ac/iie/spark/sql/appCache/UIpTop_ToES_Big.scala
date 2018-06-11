package cn.ac.iie.spark.sql.appCache

import cn.ac.iie.spark.sql.utils.{DateUtil, SessionUtil}
import org.apache.log4j.{Level, Logger}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by Administrator on 2018/4/11.
  */
object UIpTop_ToES_Big {
  def main(args: Array[String]): Unit = {
    val time=DateUtil.getUpdateTime("")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SessionUtil.createSparkSession("UIpTop_ToES")
    val date = DateUtil.getYesterdayTime(args(0))
    spark.sql("use ifp_source")


    val df_big = spark.sql("select '1' as rowid,cache_type,webSiteID," +
      "webSiteName,user_ip,sum(all_flow) as all_flow,flowUnit,sum(all_num) as all_num" +
      ",'"+time+"' as update_datetime,'1d' as business_time_type,ds as business_time" +
      " from TB_DW_Cache_big where ds = '"+date+"' group by cache_type,webSiteID,webSiteName,user_ip,flowUnit,ds")
    df_big.createOrReplaceTempView("temp11")
    val df_temp_big = spark.sql("select rowid,cache_type,websiteid,websitename" +
      ",user_ip,all_flow,all_num,update_datetime,business_time_type,business_time,row_number()OVER(partition by websitename order by all_flow desc)as rank " +
      "from temp11")
    df_temp_big.createOrReplaceTempView("temp12")
    val df_b=spark.sql("select rowid,cache_type,webSiteID,webSiteName,user_ip,all_flow,all_num,update_datetime,business_time_type,business_time from temp12 where rank <=10")
    EsSparkSQL.saveToEs(df_b,"ifp_cache_"+date+"_big/TB_APP_Cache_uiptop_1d")


    spark.stop()
  }
}
