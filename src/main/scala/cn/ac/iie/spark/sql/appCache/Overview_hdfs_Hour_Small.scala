package cn.ac.iie.spark.sql.appCache

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.ac.iie.Service.StreamingContextService
import cn.ac.iie.spark.sql.utils.{DateUtil, SessionUtil}
import cn.ac.iie.spark.streaming.scala.base.TB_SA_Cache_Small
import cn.ac.iie.spark.streaming.scala.kafka.Overview_Hour_ToES.getTime
import cn.ac.iie.spark.streaming.util.CacheSourceTools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.elasticsearch.spark.sql.EsSparkSQL

object Overview_hdfs_Hour_Small {

  def main(args: Array[String]): Unit = {

    //加载配置
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = StreamingContextService.createSparkSession("Overview_Hour_Small")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("user.name", "hadoop-user")
//    spark.sql("use ifp_source")
    //读取文件
    val timeSplit= CacheSourceTools.getFiveTime(CacheSourceTools.getNowDateIncloudmm()).split(":")
    val time=getTime(timeSplit(0))
//    var path ="hdfs://10.0.30.101:8020/ifp_source/DataCenter/SA/TB_SA_Cache_small/"+time._1+"/330000/"
    var path ="hdfs://10.0.30.101:8020/ifp_source/DataCenter/SA/TB_SA_Cache_small/20180414/330000"
    import spark.implicits._
        val load: DataFrame = spark.read.format("orc").load("G:\\atest\\ifp_source\\fromhdfs\\*.orc")
    //    val load: DataFrame = spark.read.format("orc").load("hdfs://10.0.30.101:8020/ifp_source/DataCenter/SA/TB_SA_Cache_small/20180414/330000/20180414160622/data/store/part-00002-0d002ffe-7963-4973-9b9c-ac1d6d8f27fb-c000.snappy.orc")
//        val load: DataFrame = spark.read.format("orc").load("hdfs://10.0.30.101:8020/ifp_source/DataCenter/SA/TB_SA_Cache_small/20180416/330000/20180416*/")
    //SQL
    load.show(1)

    val map = load.map(info => {
      var timeDelay:String = "null"
      if (info(35) != null && info(37) != null && info(35).toString.toUpperCase.equals("NULL") == false && info(37).toString.toUpperCase.equals("NULL") == false) {
        timeDelay=""+CacheSourceTools.getDate(info(35).toString, info(37).toString)
//        info(37) = timeDelay
      }
      TB_SA_Cache_Small(
        info(0).toString,
        info(1).toString,
        info(2).toString,
        info(3).toString,
        info(4).toString,
        info(5).toString,
        info(6).toString,
        info(7).toString,
        info(8).toString,
        info(9).toString,
        info(10).toString,
        info(11).toString,
        info(12).toString,
        info(13).toString,
        info(14).toString,
        info(15).toString,
        info(16).toString,
        info(17).toString,
        info(18).toString,
        info(19).toString,
        info(20).toString,
        info(21).toString,
        info(22).toString,
        info(23).toString,
        info(24).toString,
        info(25).toString,
        info(26).toString,
        info(27).toString,
        info(28).toString,
        info(29).toString,
        info(30).toString,
        info(31).toString,
        info(32).toString,
        info(33).toString,
        info(34).toString,
        info(35).toString,
        info(36).toString,
        timeDelay.toString,
        info(38).toString,
        "small",
        info(40).toString,
        info(41).toString,
        info(42).toString,
        info(43).toString,
        info(44).toString,
        info(45).toString,
        info(46).toString
      )
    }).toDF()
    map.createOrReplaceTempView("caches")
//    load.createOrReplaceTempView("caches")
    val sql: DataFrame  = spark.sql("select 'huawei' as factory,'1h' as business_time_type"+
      ", '"+time._2+"' as business_time " +
      ",'"+CacheSourceTools.getUpdateTime("")+"' as  update_datetime,sum(all_flow) as all_flow,avg(fstpack_delay) as fstpack_delay" +
      ",avg(backfirstresponsetime) as back_fstpack_delay,sum(back_flow) as back_flow,'small' as cache_type" +
      ",city,access_type,user_terminal,webSiteID,webSiteName" +
      ",host as domain" +
      ",deviceip as cache_eqp_info" +
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
      " from caches  " +
      " group by cache_type,deviceip,city,access_type,user_terminal,webSiteID,webSiteName,host  ")
    //写入ES
//    EsSparkSQL.saveToEs(sql, "ifp_cache_test_small/TB_APP_Cache_Overview_1h")
    load.show(1)
    /*
    val sql = spark.sql("select 'huawei' as factory,'1h' as business_time_type"+
      ", '"+time._2+"' as business_time " +
      ",'"+CacheSourceTools.getUpdateTime("")+"' as  update_datetime,sum(all_flow) as all_flow,avg(fstpack_delay) as fstpack_delay" +
      ",avg(back_fstpack_delay) as back_fstpack_delay,sum(back_flow) as back_flow,'small' as cache_type" +
      ",cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName" +
      ",domain" +
      ",(sum(all_flow)-sum(hit_flow)-sum(cacheMiss_flow)) as other_flow" +
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
      " from caches  where business_time like '"+time._2+"%' and business_time_type='5m'" +
      " group by cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain ,back_fstpack_delay ")
    val date = CacheSourceTools.getNowDate()
    if(date.endsWith("00")){
      val sj = new SimpleDateFormat("yyyyMMdd")
      val instance = Calendar.getInstance()
      instance.setTime(sj.parse(date));
      instance.add(Calendar.DATE, -1)
      val day: String = sj.format(instance.getTime)
      EsSparkSQL.saveToEs(sql, "ifp_cache_" + day + "_small/TB_APP_Cache_Overview_1h")
    }else{
      EsSparkSQL.saveToEs(sql, "ifp_cache_" + date + "_small/TB_APP_Cache_Overview_1h")
    }
    */
  }

  //根据当前时间加以判断返回（天，小时）
  def getTime(str:String): (String,String) ={
    if(str.endsWith("00")){
      val sj = new SimpleDateFormat("yyyyMMdd")
      val split = str.split(" ")
      val d:Date = sj.parse(split(0))
      val instance = Calendar.getInstance()
      instance.setTime(d);
      instance.add(Calendar.DATE, -1)
      val day: String = sj.format(instance.getTime)
      (day,day+" 23")
    }else{
      val time = str.split(" ")
      (time(0),str)
    }
  }

  def getDay()={


  }

}
