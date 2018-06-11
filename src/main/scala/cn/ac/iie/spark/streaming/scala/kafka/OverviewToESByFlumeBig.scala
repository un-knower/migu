package cn.ac.iie.spark.streaming.scala.kafka

import java.io.File
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import cn.ac.iie.spark.streaming.scala.kafka.Impl.SavaDataImpl
import cn.ac.iie.spark.streaming.util.{CacheSourceTools, IpTools, SparkSessionSingleton, WebSiteInfo}
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.sql.EsSparkSQL
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebChanelInfo
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object OverviewToESByFlumeBig {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val properties = CacheSourceTools.loadProperties()
    val conf = new SparkConf().setAppName("OverviewToESByFlume_Big")
//      .setMaster(properties.getProperty("spark.master"))
      .setMaster("local[*]")
      .set("spark.executor.memory", "5g")
//      .set("spark.cores.max", properties.getProperty("runtime.cores.max"))
//      .set("spark.cores.max", "5")
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))
      .set("user.name", properties.getProperty("user.name"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("es.index.auto.create", "true")
      .set("spark.sql.parquet.compression.codec", "snappy")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(60))
    val spark: SparkSession = SparkSessionSingleton.getInstance(conf)
    import spark.implicits._
    //广播变量
    @transient
    val ipwebinfoArray= getWebSiteInfo
    //WEB Host匹配规则
    @transient
    val ipwebBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(ipwebinfoArray)
    //IP规则
//    var ipRulesArray = IpTools.getIPControl()
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(IpTools.getIPControl())
    //WebChanel Info
    @transient
    val webChanelInfoArray=getWebChanelInfo
    @transient
    val chanelInfoBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(webChanelInfoArray)
//    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, properties.getProperty("flume.local.node1"), 20000)    //val flumeStream =   .createStream(streamingContext, [chosen machine's hostname], [chosen port])
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "10.0.30.106", 20000)
//    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "10.0.10.11", 20000)
    val dateJustmm = CacheSourceTools.getNowDateJustmm
    val map1: DStream[Long] = flumeStream.count().map(cnt =>cnt)
//    map1.map(cnt=>"Received " + cnt + " flume events." ).print()+"date:"+dateJustmm
    if (map1.count() != 0||flumeStream!=null||flumeStream!="") {
      try {
        flumeStream.foreachRDD(lines => {
          import spark.implicits._
          val mapResult = lines
            .map(recourd => {
              val str: String = new String(recourd.event.getBody.array())
              str.split("\\|")
//              str.split("' ")
            })

          //错误数据
          //val falseInfo = mapResult.filter(line => line.length < 38)
          //正确数据
          val filter = mapResult.filter(line => line.length >= 38)
          val trueInfo = filter
            .filter(lines => {
              lines(34).replaceAll("'","").toLowerCase=="null"||lines(14).replaceAll("'","").toLowerCase=="null"||lines(34).replaceAll("'","")==""||lines(14).replaceAll("'","")==""||StringUtils.isNumeric(lines(14).replaceAll("'",""))  == true && StringUtils.isNumeric(lines(34).replaceAll("'",""))  == true
              //              lines(34).toLowerCase=="null"||lines(14).toLowerCase=="null"||lines(34)==""||lines(14)==""||StringUtils.isNumeric(lines(14))  == true && StringUtils.isNumeric(lines(34))  == true &&Pattern.matches("\\d+\\.\\d+\\.\\d+\\.\\d+",lines(3))//lines(37) != "NULL"&&lines(11)!=""&&lines(35) != "NULL" && lines(34) != "0" && lines(34) != ""&&lines(14) != "NULL" && lines(14) != "0" && lines(14) != ""
            })
            .map(info => {
              CacheSourceTools.getCacheSourceEntity(1, info(2).replaceAll("'",""), info(21).replaceAll("'",""), info(6).replaceAll("'",""), info(12).replaceAll("'",""), info(4).replaceAll("'",""), info(14).replaceAll("'",""), info(34).replaceAll("'",""), info(11).replaceAll("'",""), info(27).replaceAll("'",""), info(29).replaceAll("'",""), info(30).replaceAll("'",""), info(16).replaceAll("'",""), info(15).replaceAll("'",""), info(8).replaceAll("'",""), info(1).replaceAll("'",""), info(35).replaceAll("'",""), info(36).replaceAll("'",""), info(37).replaceAll("'",""), info(17).replaceAll("'",""), ipRulesBroadcast, ipwebBroadcast, info(28).replaceAll("'",""), info(25).replaceAll("'",""), "big")
              //              CacheSourceTools.getCacheSourceEntity(1, info(2), info(21), info(6), info(12), info(4), info(14), info(34), info(11), info(27), info(29), info(30), info(16), info(15), info(8), info(1), info(35), info(36), info(37), info(17), ipRulesBroadcast, ipwebBroadcast, info(28),info(25),"big")
          })
          //五分钟报表
          val inputDF: DataFrame = trueInfo.toDF()
          inputDF.createOrReplaceTempView("bigcaches")
//          val result: DataFrame = spark.sql("select 'huawei' as factory,file_type,file_type,  flowUnit,sum(all_flow) as all_flow,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time,cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain,business_time,count(1) as all_num,sum(req_ok_num) as req_ok_num,(count(1)-sum(req_ok_num))as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num,sum(req_Error_num+req_Denied_num)as req_Otherwise_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow,sum(denied_flow) as denied_flow,sum(miss_flow) as miss_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num,sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed from bigcaches group by cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain ,flowUnit,all_flow,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time,file_type ")
          val result = spark.sql("select 'huawei' as factory,business_time_1d,business_time_1h,file_type,flowUnit,sum(all_flow) as all_flow,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time,cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain,business_time,count(1) as all_num,sum(req_ok_num) as req_ok_num,(count(1)-sum(req_ok_num))as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num,sum(req_Error_num+req_Denied_num)as req_Otherwise_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow,sum(denied_flow) as denied_flow,sum(miss_flow) as miss_flow,(sum(all_flow)-sum(hit_flow)-sum(cacheMiss_flow)) as other_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num,sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed from bigcaches group by cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain ,flowUnit,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time,business_time_1h,business_time_1d,file_type")
          val count: Long  = result.count()
          if(count>0){
//            result.show()
//            EsSparkSQL.saveToEs(result, "ifp_cache_" + CacheSourceTools.getNowDate() + "_big/TB_APP_Cache_Overview_5m")
            println("ES写入成功"+CacheSourceTools.getNowDateJustmm+"共:"+count+"条")
          }
          //20171218
          val dateDay = CacheSourceTools.getNowDate()
          //201712181715
          val justmm = getDate(CacheSourceTools.getFiveDate(dateJustmm))
          val map = filter.map(lines => {
            CacheSourceTools.SACacheBigInfo(lines: Array[String], ipRulesBroadcast, ipwebBroadcast, chanelInfoBroadcast)
          }).toDF()
          if(map.count>0){
            var utils=new GetTimeUtils()
            var path="hdfs://10.0.30.101:8020/ifp_source/DataCenter/SA/TB_SA_Cache_big/"+CacheSourceTools.getNowDate()+"/330000/"+utils.getTime.replaceAll(":","")+"/data/store/"
//            var path="hdfs://10.0.10.2:8020/ifp_source/DataCenter/SA/TB_SA_Cache_big/"+CacheSourceTools.getNowDate()+"/100000/"+utilss.getTime.replaceAll(":","")+"/data/store/"
//            SavaDataImpl.SaveDataToHDFS(map,path)
          }
//          val saDF = saEntity.toDF()
          //数据写入hdfs
//          if(saDF.count()>0)
//          {
            //result.write.parquet("/ifp_source/DataCenter/sa/TB_SA_CACHE/"+CacheSourceTools.getNowDate()+"/small/"+CacheSourceTools.getNowDateJustmm()+"/data/correct_store/330000")
//            saEntity.toDF().write.parquet("G:\\atest/ifp_source/DataCenter/sa/TB_SA_CACHE/"+CacheSourceTools.getNowDate()+"/small/"+justmm+"/data/correct_store/330000")
            //saDF.write.parquet("G:\\atest/ifp_source/DataCenter/sa/TB_SA_CACHE/330000/"+dateDay+"/small/"+justmm+"/data/correct_store")
//          }
          //脏数据存储路径
//          val falseDF = falseInfo.toDF()
//          println(falseDF.count())
//          if(falseDF.count()>0){
            //falseDF.write.parquet("G:\\atest/ifp_source/DataCenter/sa/TB_SA_CACHE/330000/"+dateDay+"/small/"+justmm+"/data/error_store")
//            falseInfo.toDF().write.parquet("/ifp_source/DataCenter/sa/TB_SA_CACHE/"+CacheSourceTools.getNowDate()+"/small/"+justmm+"/data/error_store/330000")
//          }

          //falseInfo.toDF().write.parquet("G:\\atest/ifp_source/DataCenter/sa/TB_SA_CACHE/"+CacheSourceTools.getNowDate()+"/small/"+justmm+"/data/error_store/330000")
          //test start
          //val base: DataFrame = inputDF.select("rowid","flowUnit","all_flow","fstpack_delay","back_fstpack_delay","back_flow","update_datetime","business_time_type","business_time","ds_type","ds")
          //val base: DataFrame = inputDF.select("rowid","flowUnit","all_flow")
          //val sqls: DataFrame = spark.sql("select rowid,cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain,business_time,count(1) as all_num,sum(req_ok_num) as req_ok_numb,(count(1)-sum(req_ok_num))as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow,sum(miss_flow) as miss_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num,sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed from caches group by rowid,cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain,business_time")
          //val sqls: DataFrame = spark.sql("select rowid,cache_type,cache_eqp_info from caches group by rowid,cache_type,cache_eqp_info")
          //val join = base.join(sqls, base("rowid") === sqls("rowid"),"left").distinct()
          //join.show()
          //EsSparkSQL.saveToEs(join, "ifp_cache_" + CacheSourceTools.getNowDate() + "_small/TB_DW_Cache_Overview_5m")
          //test end
          //CacheSourceTools.getResultDataFrame(map.toDS(),spark)
          //map.toDF().show()
          //map.toDS().createOrReplaceTempView("caches")
          //spark.sql("select rowid from caches").show()
          //map.toDF().createGlobalTempView("caches")
          //map.toDF().printSchema()
          //spark.sql("select rowid from caches").show()
          //val join: DataFrame = CacheSourceTools.getResultDataFrame(map.toDF(),spark)
          //join.show()
          //EsSparkSQL.saveToEs(join, "ifp_cache_" + CacheSourceTools.getNowDate() + "_flume/es5m")
          //EsSparkSQL.saveToEs(map.toDS(), "ifp_cache_" + CacheSourceTools.getNowDate() + "_flume/es5m")
        })
      }catch {
        case e: Exception => "本次写入失败"+dateJustmm
      }

    }
    ssc.start()
    ssc.awaitTermination()
  }

  def getDate(str:String): String ={
    val dayTime: String = str.substring(0, str.length - 4)
    val mmTime = str.substring(str.length-4,str.length)
//    println("天:"+dayTime+"时:"+mmTime)
    if(mmTime.startsWith("00")){
      val sj = new SimpleDateFormat("yyyyMMdd")
      val d:Date = sj.parse(dayTime)
      val instance = Calendar.getInstance()
      instance.setTime(d)
      instance.add(Calendar.DATE, -1)
      val passDay: String = sj.format(instance.getTime)
      passDay+"55"
    }else{
      str
    }
  }

}
