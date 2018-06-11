package cn.ac.iie.spark.sql.appCache

import cn.ac.iie.spark.sql.utils.{DateUtil, SessionUtil}
import org.apache.log4j.{Level, Logger}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by Administrator on 2018/4/11.
  */
object UIpCity_ToES_Big {
  def main(args: Array[String]): Unit = {
    val time=DateUtil.getUpdateTime("")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SessionUtil.createSparkSession("UIpCity_ToES")
    val date = DateUtil.getYesterdayTime(args(0))
    spark.sql("use ifp_source")


    val df_big=spark.sql("select '1' as rowid,cache_type,webSiteID,webSiteName,city" +
      ",count(distinct(user_ip)) as userip_num,'"+time+"' as update_datetime,'1d' as business_time_type" +
      ",ds as business_time from TB_DW_Cache_big where ds = '"+date+"' " +
      "group by cache_type,webSiteID,webSiteName,city,ds")
    EsSparkSQL.saveToEs(df_big,"ifp_cache_"+date+"_big/TB_APP_Cache_uipcity_1d")


    spark.stop()
  }
}
