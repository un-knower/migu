package cn.ac.iie.spark.sql.appCache

import java.util.Random

import cn.ac.iie.spark.sql.utils.DateUtil
import cn.ac.iie.spark.streaming.util.CacheSourceTools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by Administrator on 2018/4/11.
  */
object UriAnalysis_ToES_Url {
  def main(args: Array[String]): Unit = {
    val time=DateUtil.getUpdateTime("")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val properties = CacheSourceTools.loadProperties()
    val spark = SparkSession.builder()
      .appName("UriAnalysis_ToES")
      .master("yarn")
      .config("spark.sql.warehouse.dir",properties.getProperty("spark.sql.warehouse.dir"))
      .config("es.index.auto.create", "true")
      .config("es.nodes", properties.getProperty("es.nodes"))
      .config("es.port", properties.getProperty("es.port"))
      .config("spark.executor.instances","4")
      .config("spark.cores.max", "15")
      .config("spark.executor.memory","10G")
        .config("spark.sql.shuffle.partitions","400")
      .enableHiveSupport()
      .getOrCreate()
    val date = DateUtil.getYesterdayTime(args(0))
    spark.sql("use ifp_source")

    val df=spark.sql("select 'uri' as rowid,factory,websiteid,websitename," +
      "uri,all_num,if(status_code like '2%' or status_code like '3%',1,0) as req_ok_num," +
      "if(cache_hit_status like 'hit',1,0) as req_Hit_num," +
      "if(cache_hit_status like 'miss',1,0) as req_miss_num," +
      "if(cache_hit_status like 'denied',1,0) as req_denied_num," +
      "if(cache_hit_status like 'error',1,0) as req_error_num," +
      "if(Status_code like '2%',1,0) as return2xx_num," +
      "if(Status_code like '3%',1,0) as return3xx_num,if(Status_code like '4%',1,0) as return4xx_num," +
      "if(Status_code like '5%',1,0) as return5xx_num,flowunit,all_flow," +
      "if(cache_hit_status like 'hit',all_flow,0) as hit_flow," +
      "if(cache_hit_status like 'miss',back_flow,0) as miss_flow," +
      "if(cache_hit_status like 'miss',all_flow,0) as miss1_flow," +
      "'"+time+"' as update_datetime,'1d' as business_time_type," +
      "ds as business_time from tb_dw_cache_big_url where ds = '"+date+"'")
    df.createOrReplaceTempView("temp")
    val df_big=spark.sql("select rowid,factory,websiteid,websitename,uri,sum(all_num) as all_num," +
      "sum(req_ok_num) as req_ok_num,sum(all_num-req_ok_num) as req_fail_num,sum(req_Hit_num) as req_Hit_num" +
      ",sum(req_miss_num) as req_miss_num,sum(req_denied_num) as req_denied_num," +
      "sum(req_error_num) as req_error_num,sum(return2xx_num) as return2xx_num," +
      "sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num," +
      "flowunit,sum(all_flow) as all_flow,sum(hit_flow) as hit_flow,sum(miss_flow) as miss_flow," +
      "sum(all_flow-hit_flow-miss1_flow) as other_flow,update_datetime,business_time_type," +
      "business_time from temp group by rowid,factory,websiteid,websitename,uri,flowunit,update_datetime," +
      "business_time_type,business_time")
    df_big.createOrReplaceTempView("temp01")
    val rand = new Random()
    val df_a=spark.sql(

        "select rowid,factory,websiteid,websitename,uri,all_num,req_ok_num,req_fail_num,req_Hit_num,req_miss_num,req_denied_num"+
        ",req_error_num,return2xx_num,return3xx_num,return4xx_num,return5xx_num,flowunit,all_flow,hit_flow,miss_flow,other_flow"+
        ",update_datetime,business_time_type,business_time,row_number()OVER(partition by factory,websiteid,websitename,floor(rand()*40) order by all_flow desc)as rank from temp01"
      )
    df_a.createOrReplaceTempView("temp02")
    val test_a=spark.sql(
      """
        |select rowid,factory,websiteid,websitename,uri,all_num,req_ok_num,req_fail_num,req_Hit_num,req_miss_num,req_denied_num
        |,req_error_num,return2xx_num,return3xx_num,return4xx_num,return5xx_num,flowunit,all_flow,hit_flow,miss_flow,other_flow
        |,update_datetime,business_time_type,business_time from temp02 where rank<=100
      """.stripMargin)
    test_a.createOrReplaceTempView("temp03")
    val test_b=spark.sql(
      """
        |select rowid,factory,websiteid,websitename,uri,all_num,req_ok_num,req_fail_num,req_Hit_num,req_miss_num,req_denied_num
        |,req_error_num,return2xx_num,return3xx_num,return4xx_num,return5xx_num,flowunit,all_flow,hit_flow,miss_flow,other_flow
        |,update_datetime,business_time_type,business_time,row_number()OVER(partition by factory,websiteid,websitename order by all_flow desc)as rank from temp03
      """.stripMargin)
    test_b.createOrReplaceTempView("temp04")
    val spark_big=spark.sql(
      """
        |select rowid,factory,webSiteID,webSiteName,uri,all_num,req_ok_num,req_fail_num,req_Hit_num,req_Miss_num
        |,return2xx_num,return3xx_num,return4xx_num,return5xx_num,flowUnit,all_flow,hit_flow,miss_flow,other_flow
        |,update_datetime,business_time_type,business_time from temp04 where rank<=100
      """.stripMargin)
    EsSparkSQL.saveToEs(spark_big,"ifp_cache_"+date+"_big/TB_APP_Cache_urianalysis_1d")


    spark.stop()
  }
}
