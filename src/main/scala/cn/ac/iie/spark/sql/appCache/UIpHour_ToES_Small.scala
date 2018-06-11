package cn.ac.iie.spark.sql.appCache

import cn.ac.iie.spark.sql.utils.{DateUtil, SessionUtil}
import org.apache.log4j.{Level, Logger}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by Administrator on 2018/4/11.
  */
object UIpHour_ToES_Small {
  def main(args: Array[String]): Unit = {
    val time=DateUtil.getUpdateTime("")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SessionUtil.createSparkSession("UIpHour_ToES_Small")
    val date = DateUtil.getYesterdayTime(args(0))
    spark.sql("use ifp_source")


    val df_small = spark.sql("select '1' as rowid,cache_type,webSiteID,webSiteName," +
      "business_hour,count(distinct(user_ip)) as userip_num,'"+time+"' as update_datetime" +
      ",'1d' as business_time_type,ds as business_time from TB_DW_Cache_small" +
      " where ds='"+date+"' group by cache_type,business_hour,webSiteID,webSiteName,ds")
    EsSparkSQL.saveToEs(df_small,"ifp_cache_"+date+"_small/TB_APP_Cache_uiphour_1d")


    spark.stop()
  }
}
