package cn.ac.iie.spark.sql.appCache

import cn.ac.iie.spark.sql.utils.{DateUtil, SessionUtil}
import org.apache.log4j.{Level, Logger}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by Administrator on 2018/4/11.
  */
object WebANS_ToES_Small {
  def main(args: Array[String]): Unit = {
    val time=DateUtil.getUpdateTime("")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SessionUtil.createSparkSession("WebANS_ToES_Small")
    val date = DateUtil.getYesterdayTime(args(0))
    spark.sql("use ifp_source")

    val df_small = spark.sql("select '1' as rowid,factory,cache_type,access_type,user_terminal," +
      "webSiteID,webSiteName,domain,file_type,business_hour,sum(all_num) as all_num,flowUnit" +
      ",sum(all_flow) as all_flow,'"+time+"' as update_datetime,'1d' as business_time_type,ds as business_time" +
      " from TB_DW_Cache_small" +
      " where ds = '"+date+"' group by factory,cache_type,access_type,user_terminal,webSiteID" +
      ",webSiteName,domain,file_type,business_hour,flowUnit,ds")
    EsSparkSQL.saveToEs(df_small,"ifp_cache_"+date+"_small/TB_APP_Cache_WebANS_1d")


    spark.stop()
  }
}
