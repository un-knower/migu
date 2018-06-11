package cn.ac.iie.spark.streaming.scala.hdfs

import cn.ac.iie.spark.streaming.scala.base.CacheOverview
import cn.ac.iie.spark.streaming.scala.kafka.GetTimeUtils
import cn.ac.iie.spark.streaming.util.WebSiteInfo
import cn.ac.iie.spark.streaming.util.{CacheSourceTools, IpTools, SparkSessionSingleton}
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object OverviewRead_Small {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val properties = CacheSourceTools.loadProperties()
    val sparkConf = new SparkConf().setAppName("OverviewToESByFlume_Small")
      .setMaster(properties.getProperty("spark.master"))
//            .setMaster("local[1]")
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("spark.executor.memory", "5g")
      .set("spark.cores.max", properties.getProperty("runtime.cores.max"))
      .set("es.port", properties.getProperty("es.port"))
      .set("user.name", properties.getProperty("user.name"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    // 创建Streaming的上下文，包括Spark的配置和时间间隔，这里时间为间隔20秒
    val ssc = new StreamingContext(sparkConf,Seconds(60))
    val spark: SparkSession = SparkSessionSingleton.getInstance(sparkConf)
    //    import spark.implicits._
    //广播变量
    //网站信息
    @transient
    val ipwebinfoArray= WebSiteInfo.getWebSiteInfo
    //WEB Host匹配规则
    @transient
    val ipwebBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(ipwebinfoArray)
    //IP规则
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(IpTools.getIPControl())

    val dateJustmm = CacheSourceTools.getNowDateJustmm
    val linesStream: DStream[String] = ssc.textFileStream(properties.getProperty("hdfs.smallfile.path"))
    // 指定监控的目录

    linesStream.foreachRDD(line=>{
      import spark.implicits._
      var map: RDD[Array[String]] = line.map(line => {
        line.split("|")
      }).filter(line => line.length == 38)
      map

    })

    val flatMap = linesStream.flatMap(_.split("|")).filter(line => line.length == 38)
    linesStream.foreachRDD(lines => {
      import spark.implicits._
      val filter = lines
        .map(recourd => {
//          val str: String = new String(recourd.toString)
          recourd.split("|")
        })
        .filter(line => line.length == 38)
      val map: RDD[CacheOverview] = filter
        .filter(lines => {
          lines(34).replaceAll("'", "").toLowerCase == "null" || lines(14).replaceAll("'", "").toLowerCase == "null" || lines(34).replaceAll("'", "") == "" || lines(14).replaceAll("'", "") == "" || StringUtils.isNumeric(lines(14).replaceAll("'", "")) == true && StringUtils.isNumeric(lines(34).replaceAll("'", "")) == true
        })
        .map(info => {
          CacheSourceTools.getCacheSourceEntity(1, info(2).replaceAll("'", ""), info(21).replaceAll("'", ""), info(6).replaceAll("'", ""), info(12).replaceAll("'", ""), info(4).replaceAll("'", ""), info(14).replaceAll("'", ""), info(34).replaceAll("'", ""), info(11).replaceAll("'", ""), info(27).replaceAll("'", ""), info(29).replaceAll("'", ""), info(30).replaceAll("'", ""), info(16).replaceAll("'", ""), info(15).replaceAll("'", ""), info(8).replaceAll("'", ""), info(1).replaceAll("'", ""), info(35).replaceAll("'", ""), info(36).replaceAll("'", ""), info(37).replaceAll("'", ""), info(17).replaceAll("'", ""), ipRulesBroadcast, ipwebBroadcast, info(28).replaceAll("'", ""), info(25).replaceAll("'", ""), "small")
        })
      val inputDF = map.toDF()
      inputDF.createOrReplaceTempView("caches")
      val date = CacheSourceTools.getNowDate()
      val result = spark.sql("select 'huawei' as factory, file_type,flowUnit,all_flow,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time,cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain,business_time,count(1) as all_num,sum(req_ok_num) as req_ok_num,(count(1)-sum(req_ok_num))as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow,sum(denied_flow) as denied_flow,sum(miss_flow) as miss_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num,sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed from caches group by cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain ,flowUnit,all_flow,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time,file_type")
      if(result!=null&&result.count()>0){
        result.show()
        EsSparkSQL.saveToEs(result, "ifp_cache_" + date + "_small/TB_APP_Cache_Overview_5m")
        println("ES写入成功"+CacheSourceTools.getNowDateJustmm+"共:"+result.count()+"条")
      }
      val dataFrame = filter.map(lines=>{
        CacheSourceTools.getSACacheInfo(lines,"huawei", ipRulesBroadcast, ipwebBroadcast)
      }).toDF()
      val saCount = dataFrame.count()
      if(saCount>0){
        var utilss=new GetTimeUtils()
        var path ="hdfs://10.0.10.2:8020/ifp_source/DataCenter/SA/TB_SA_Cache_small/"+date+"/100000/"+utilss.getTime.replaceAll(":","")+"/data/store/"
//        dataFrame.write.parquet(path)
        println("写入数据到路径:"+path)
        println("HDFS写入成功"+CacheSourceTools.getNowDateJustmm+"共:"+saCount+"条")
      }
    })

    






//    val  words : DStream[String] = lines.flatMap(_.split("|"))
    //.filter(line => line.length > 3)


//    val map: DStream[Array[String]] = lines.map(line => {
//      line.split(" ")
//    })
//    map.print()

//    val wordCount: DStream[(String, Int)] = words.map(x => (x,1)).reduceByKey(_+_)
//    val wordCount = map.map(x => (x,1)).reduceByKey(_+_)
//    println(wordCount.context)
    //    wordCount.saveAsTextFiles("hdfs://10.149.252.106:9000/output/")

    ssc.start()
    ssc.awaitTermination()

  }

  def test(): Unit ={
/*    //网站信息
    @transient
    val ipwebinfoArray= getWebSiteInfo
    //WEB Host匹配规则
    @transient
    val ipwebBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(ipwebinfoArray)
    //IP规则
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(IpTools.getIPControl())

    val spark: SparkSession = SparkSessionSingleton.getInstance(conf)
    val lines: DStream[String] = ssc.textFileStream("G:\\ifp_source\\test")
    lines.foreachRDD(line=>{
      import spark.implicits._
      val words = line.map(_.split("' "))//.filter(line => line.length == 38)
      println(words.count())
      val resultMap: RDD[CacheOverview] = words.filter(lines => {
        lines(34).toString.replaceAll("'", "").toLowerCase == "null" || lines(14).toString.replaceAll("'", "").toLowerCase == "null" || lines(34).toString.replaceAll("'", "") == "" || lines(14).toString.replaceAll("'", "") == "" || StringUtils.isNumeric(lines(14).toString.replaceAll("'", "")) == true && StringUtils.isNumeric(lines(34).toString.replaceAll("'", "")) == true
      }).map(info => {
        CacheSourceTools.getCacheSourceEntity(1, info(2).toString.replaceAll("'", ""), info(21).toString.replaceAll("'", ""), info(6).toString.replaceAll("'", ""), info(12).toString.replaceAll("'", ""), info(4).toString.replaceAll("'", ""), info(14).toString.replaceAll("'", ""), info(34).toString.replaceAll("'", ""), info(11).toString.replaceAll("'", ""), info(27).toString.replaceAll("'", ""), info(29).toString.replaceAll("'", ""), info(30).toString.replaceAll("'", ""), info(16).toString.replaceAll("'", ""), info(15).toString.replaceAll("'", ""), info(8).toString.replaceAll("'", ""), info(1).toString.replaceAll("'", ""), info(35).toString.replaceAll("'", ""), info(36).toString.replaceAll("'", ""), info(37).toString.replaceAll("'", ""), info(17).toString.replaceAll("'", ""), ipRulesBroadcast, ipwebBroadcast, info(28).toString.replaceAll("'", ""), info(25).toString.replaceAll("'", ""), "small")
      })
      val inputDF =resultMap.toDF
      println(inputDF.count())
      inputDF.show()
    }
    )*/
  }

}
