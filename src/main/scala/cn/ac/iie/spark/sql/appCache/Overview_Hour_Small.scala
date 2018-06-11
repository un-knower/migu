package cn.ac.iie.spark.sql.appCache

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.ac.iie.spark.streaming.util.{CacheSourceTools, SparkSessionSingleton}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

object Overview_Hour_Small {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val properties = CacheSourceTools.loadProperties()
    val conf = new SparkConf().setAppName("Overview_Hour_Small")
      .setMaster(properties.getProperty("spark.master"))
//      .setMaster("local[*]")
//      .set("spark.cores.max", "3")
      .set("spark.executor.memory", properties.getProperty("hour.executor.memory"))
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", properties.getProperty("es.nodes"))
    conf.set("es.port", properties.getProperty("es.port"))
    conf.set("user.name", properties.getProperty("user.name"))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)
    val spark: SparkSession = SparkSessionSingleton.getInstance(conf)

    val timeSplit= CacheSourceTools.getFiveTime(CacheSourceTools.getNowDateIncloudmm()).split(":")
    val time=if(args.length==2) (args(0).toString,args(1).toString) else getTime(timeSplit(0))
//    val options = Map("pushdown" -> "true", "es.nodes" -> properties.getProperty("es.nodes"), "es.port" -> properties.getProperty("es.port"))
//    val options = Map("pushdown" -> "true", "es.nodes" -> "10.0.5.239", "es.port" -> properties.getProperty("es.port"))
//    val loadDF = context.read.options(options).format("org.elasticsearch.spark.sql").load("ifp_cache_"+time._1+"_small/TB_APP_Cache_Overview_5m")//TB_APP_Cache_Overview_5m
    val loadDF =context.esDF("ifp_cache_"+time._1+"_small/TB_APP_Cache_Overview_5m","{\"query\":{\"bool\":{\"must\":[{\"term\":{\"business_time_1h\":\""+getNowDateJustHH()+"\"}},{\"term\":{\"business_time_type\":\"5m\"}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"sort\":[],\"aggs\":{}}")
    loadDF.createOrReplaceTempView("caches")
    loadDF.show()
    println("日期："+time._1+"读取"+getNowDateJustHH()+"时间段内的数据")
    val sql = spark.sql("select 'huawei' as factory,'1h' as business_time_type"+
      ", '"+time._2+"' as business_time " +
      ",'"+CacheSourceTools.getUpdateTime("")+"' as  update_datetime,sum(all_flow) as all_flow,avg(fstpack_delay) as fstpack_delay" +
      ",avg(back_fstpack_delay) as back_fstpack_delay,sum(back_flow) as back_flow,'small' as cache_type" +
      ",cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName" +
      ",domain" +
      ",sum(other_flow) as other_flow" +
      ",sum(all_num) as all_num" +
      ",'Kb' as flowUnit"+
      ",sum(req_ok_num) as req_ok_num" +
      ",'1h' as business_time_type" +
      ",sum(req_fail_num) as req_fail_num"+ //
      ",sum(denied_flow) as denied_flow"+ //
      ",sum(req_Hit_num) as req_Hit_num" +
      ",sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num" +
      ",sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num" +
      ",sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num" +
      ",sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow" +
      ",sum(miss_flow) as miss_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num" +
      ",sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num" +
      ",sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed " +
      " from caches " +
      " group by cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain ")

    val date = CacheSourceTools.getNowDate()
    if(date.endsWith("00")){
      val sj = new SimpleDateFormat("yyyyMMdd")
      val instance = Calendar.getInstance()
      instance.setTime(sj.parse(date))
      instance.add(Calendar.DATE, -1)
      val day: String = sj.format(instance.getTime)
      EsSparkSQL.saveToEs(sql, "ifp_cache_" + day + "_small/TB_APP_Cache_Overview_1h")
    }else{
      EsSparkSQL.saveToEs(sql, "ifp_cache_" + date + "_small/TB_APP_Cache_Overview_1h")
//      EsSparkSQL.saveToEs(sql, "test_small/TB_APP_Cache_Overview_1h")
    }
}
  //根据当前时间加以判断返回（天，小时）
  def getTime(str:String): (String,String) ={
    if(str.endsWith("00")){
      val sj = new SimpleDateFormat("yyyyMMdd")
      val split = str.split(" ")
      val d:Date = sj.parse(split(0))
      val instance = Calendar.getInstance()
      instance.setTime(d)
      instance.add(Calendar.DATE, -1)
      val day: String = sj.format(instance.getTime)
      (day,day+" 23")
    }else{
      val time = str.split(" ")
      (time(0),str)
    }
  }

  def getNowDateJustHH():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd HH")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.HOUR_OF_DAY,-1)
    var current = dateFormat.format(cal.getTime())
    current
  }

}
