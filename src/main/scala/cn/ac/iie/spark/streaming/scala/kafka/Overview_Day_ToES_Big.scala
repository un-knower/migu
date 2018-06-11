package cn.ac.iie.spark.streaming.scala.kafka

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.ac.iie.spark.streaming.util.{CacheSourceTools, SparkSessionSingleton}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

object Overview_Day_ToES_Big {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val properties = CacheSourceTools.loadProperties()
    val conf = new SparkConf().setAppName("Overview_Day_Big")
    .setMaster(properties.getProperty("spark.master"))
//    .setMaster("local[*]")
    .set("es.index.auto.create", "true")
    .set("spark.executor.memory", properties.getProperty("day.executor.memory"))
    .set("es.nodes", properties.getProperty("es.nodes"))
    .set("es.port", properties.getProperty("es.port"))
    .set("spark.cores.max", properties.getProperty("day.cores.max"))
    .set("user.name", properties.getProperty("user.name"))
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)
    import context.implicits._
    val spark: SparkSession = SparkSessionSingleton.getInstance(conf)
    val timeSplit= CacheSourceTools.getFiveTime(CacheSourceTools.getNowDateIncloudmm()).split(" ")
    val time=getTime(timeSplit(0))
    val loadDF: DataFrame = context.read.format("org.elasticsearch.spark.sql").load("ifp_cache_"+time._2+"_big/TB_APP_Cache_Overview_1h")
//    val loadDF: DataFrame = context.read.format("org.elasticsearch.spark.sql").load("ifp_cache_20180118_big/TB_APP_Cache_Overview_1h")
    loadDF.createOrReplaceTempView("caches")
    //loadDF.show()
    println("日期："+time._1+"读取"+time._2+"的数据")
    import spark.implicits._
    val sql = spark.sql("select '' as domain,'huawei' as factory,'big' as cache_type,'' as file_type, '"+time._2+"' as business_time,'"+CacheSourceTools.getUpdateTime("")+"' as update_datetime,'Kb'as flowUnit,sum(all_flow) as all_flow ,sum(other_flow) as other_flow,avg(fstpack_delay) as fstpack_delay,sum(denied_flow) as denied_flow,avg(back_fstpack_delay) as back_fstpack_delay,sum(back_flow) as back_flow, '1d' as  business_time_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,sum(all_num) as all_num,sum(req_ok_num) as req_ok_num,sum(req_fail_num) as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow,sum(miss_flow) as miss_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num,sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed from caches  where business_time_type='1h' group by cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,back_fstpack_delay ")
//    val sql = spark.sql("select 'huawei' as factory,'big' as cache_type,file_type, '20180118 10' as business_time,'"+CacheSourceTools.getUpdateTime("")+"' as update_datetime,flowUnit,sum(all_flow) as all_flow     ,avg(fstpack_delay) as fstpack_delay,sum(denied_flow) as denied_flow,avg(back_fstpack_delay) as back_fstpack_delay,sum(back_flow) as back_flow, '1d' as  business_time_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain,sum(all_num) as all_num,sum(req_ok_num) as req_ok_num,sum(req_fail_num) as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow,sum(miss_flow) as miss_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num,sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed from caches  where business_time_type='1h' group by cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain ,flowUnit,back_fstpack_delay,file_type ")
//    val sql = spark.sql("select '"+time._1+"' as business_time,flowUnit,sum(all_flow) as all_flow     ,avg(fstpack_delay) as fstpack_delay,sum(denied_flow) as denied_flow,avg(back_fstpack_delay) as back_fstpack_delay,sum(back_flow) as back_flow, '1d' as  business_time_type,cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain,business_time,sum(all_num) as all_num,sum(req_ok_num) as req_ok_num,sum(req_fail_num) as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow,sum(miss_flow) as miss_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num,sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed from caches  where business_time_type='5m' group by cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain ,flowUnit,back_fstpack_delay ")
//    sql.show()
    EsSparkSQL.saveToEs(sql, "ifp_cache_" + time._2 + "_big/TB_APP_Cache_Overview_1d")
//    EsSparkSQL.saveToEs(sql, "ifp_cache_20180118_big/TB_APP_Cache_Overview_1d")
  }
  //根据当前时间加以判断返回（天，小时）
  def getTime(str:String): (String,String) ={

    val sj = new SimpleDateFormat("yyyyMMdd")
    val d:Date = sj.parse(str)
    val instance = Calendar.getInstance()
    instance.setTime(d)
    instance.add(Calendar.DATE, -1)
    val passDay: String = sj.format(instance.getTime)
    (str,passDay)
  }

}
