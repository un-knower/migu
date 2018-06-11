package cn.ac.iie.spark.sql.appCache

import cn.ac.iie.spark.sql.utils.DateUtil
import cn.ac.iie.spark.streaming.util.CacheSourceTools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by Administrator on 2018/4/11.
  */
object UIpTop_ToES_Small {
  def main(args: Array[String]): Unit = {
    val time=DateUtil.getUpdateTime("")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val properties = CacheSourceTools.loadProperties()
    val spark = SparkSession.builder()
      .appName("UIpTop_ToES_Small")
      .master("yarn")
      .config("spark.sql.warehouse.dir",properties.getProperty("spark.sql.warehouse.dir"))
      .config("es.index.auto.create", "true")
      .config("es.nodes", properties.getProperty("es.nodes"))
      .config("es.port", properties.getProperty("es.port"))
      .config("spark.executor.instances","4")
      .config("spark.cores.max", "10")
      .config("spark.executor.memory","4G")
      //      .config("spark.executor.instances","4")
      //      .config("spark.cores.max", "12")
      .enableHiveSupport()
      .getOrCreate()
    val date = DateUtil.getYesterdayTime(args(0))
    spark.sql("use ifp_source")


    val df_small = spark.sql("select '1' as rowid,cache_type,webSiteID," +
      "webSiteName,user_ip,sum(all_flow) as all_flow,flowUnit,sum(all_num) as all_num" +
      ",'"+time+"' as update_datetime,'1d' as business_time_type,ds as business_time" +
      " from TB_DW_Cache_small where ds = '"+date+"' group by cache_type,webSiteID,webSiteName,user_ip,flowUnit,ds")
    df_small.createOrReplaceTempView("temp01")
    val df_temp_small = spark.sql("select rowid,cache_type,websiteid,websitename" +
      ",user_ip,all_flow,all_num,update_datetime,business_time_type,business_time,row_number()OVER(partition by websitename order by all_flow desc)as rank " +
      "from temp01")
    df_temp_small.createOrReplaceTempView("temp02")
    val df_s=spark.sql("select rowid,cache_type,webSiteID,webSiteName,user_ip,all_flow,all_num,update_datetime,business_time_type,business_time from temp02 where rank <=10")
    EsSparkSQL.saveToEs(df_s,"ifp_cache_"+date+"_small/TB_APP_Cache_uiptop_1d")


    spark.stop()
  }
}
