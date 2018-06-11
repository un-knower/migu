package cn.ac.iie.spark.streaming.scala.kafka.Impl

import cn.ac.iie.spark.streaming.scala.base.CacheOverview
import cn.ac.iie.spark.streaming.scala.kafka.GetTimeUtils
import cn.ac.iie.spark.streaming.scala.kafka.Service.StreamingContextService
import cn.ac.iie.spark.streaming.util.{CacheSourceTools, IpTools, SparkSessionSingleton}
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

object SmallFileAnalysis {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val properties = CacheSourceTools.loadProperties()

  //IP规则
  val ipRulesArray = IpTools.getIPControl()

  def OverrideToEsByHDFS(path:String): Unit ={

    val ssc=StreamingContextService.createHDFSContext(path)
    var ipwebinfoArray: HashMap[String, (String, String, String, String, String, String)] = getWebSiteInfo
    var ipwebBroadcast= ssc.sparkContext.broadcast(ipwebinfoArray)
    var ipRulesBroadcast = ssc.sparkContext.broadcast(ipRulesArray)

  }

  def OverrideToEsByFlume(): Unit ={
    val conf= StreamingContextService.createSparkConf("OverviewToESByFlume_Small")
    val ssc = StreamingContextService.createFlumeReceiverContext(conf)
    val spark: SparkSession = SparkSessionSingleton.getInstance(conf)
    //广播变量
    //网站信息
    @transient
    val ipwebinfoArray= getWebSiteInfo
    //WEB Host匹配规则
    @transient
    val ipwebBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(ipwebinfoArray)
    //IP规则
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(IpTools.getIPControl())
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, properties.getProperty("flume.local.node2"), 19999)    //val flumeStream =   .createStream(streamingContext, [chosen machine's hostname], [chosen port])
    val map1: DStream[Long] = flumeStream.count().map(cnt =>cnt)
    val dateJustmm = CacheSourceTools.getNowDateJustmm
    println("ES更新时间"+CacheSourceTools.getNowDateIncloudmm()+"共:")
    map1.map(cnt=>"Received " + cnt + " flume events." ).print()+"date:"+dateJustmm
    if (map1 != 0||flumeStream!=null||flumeStream!="") {
      try {
        flumeStream.foreachRDD(lines => {
          import spark.implicits._
          val filter = lines
            .map(recourd => {
              val str: String = new String(recourd.event.getBody.array())
              str.split("' ")
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
          if(result!=null){
            EsSparkSQL.saveToEs(result, "ifp_cache_" + date + "_small/TB_APP_Cache_Overview_5m")
            println("ES写入成功"+CacheSourceTools.getNowDateJustmm+"共:"+result.count()+"条")
          }
          val dataFrame = filter.map(lines=>{
            CacheSourceTools.getSACacheInfo(lines,"huawei", ipRulesBroadcast, ipwebBroadcast)
          }).toDF()
          var utils=new GetTimeUtils()
          var path ="hdfs://10.0.10.2:8020/ifp_source/DataCenter/SA/TB_SA_Cache_small/"+date+"/100000/"+utils.getTime.replaceAll(":","")+"/data/store/"
          SavaDataImpl.SaveDataToHDFS(dataFrame,path)
        })
      }catch {
        case e: Exception => "本次写入失败:"+e.printStackTrace()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def jiangsiFilter(): Unit ={

  }




}
