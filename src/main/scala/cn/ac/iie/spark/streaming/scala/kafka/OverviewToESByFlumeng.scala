package cn.ac.iie.spark.streaming.scala.kafka

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.ac.iie.spark.streaming.scala.base.CacheOverview
import cn.ac.iie.spark.streaming.util.{CacheSourceTools, DateTools, IpTools, SparkSessionSingleton}
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.util.DoubleAccumulator
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object OverviewToESByFlumeng {

  def main(args: Array[String]): Unit = {
    val properties = CacheSourceTools.loadProperties()
    val conf = new SparkConf().setAppName("OverviewToHdfsAndESByFlume")
      .setMaster("spark://10.0.30.101:7077")
//      .setMaster("local[3]")
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))
      .set("user.name", properties.getProperty("user.name"))
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(50))
    val spark: SparkSession = SparkSessionSingleton.getInstance(conf)
    import spark.implicits._
    //广播变量
    @transient
//    val ipwebinfoArray= getWebSiteInfo
    //WEB Host匹配规则
    @transient
//    val ipwebBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(ipwebinfoArray)
    //IP规则
//    var ipRulesArray = IpTools.getIPControl()
//    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(IpTools.getIPControl())
    //val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "10.0.30.106", 19999)    //val flumeStream =   .createStream(streamingContext, [chosen machine's hostname], [chosen port])
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "10.0.30.107", 19999)    //val flumeStream =   .createStream(streamingContext, [chosen machine's hostname], [chosen port])
    //flumeStream.count().map(cnt => "Received " + cnt + " flume events.").print()
    val map1: DStream[Long] = flumeStream.count().map(cnt =>cnt)

    map1.map(cnt=>"Received " + cnt + " flume events." ).print()
    println(map1.count())
   /*
    if (map1.count() != 0||flumeStream!=null||flumeStream!="") {
      try {
            flumeStream.foreachRDD(lines => {
              import spark.implicits._
              val mapResult = lines
                .map(recourd => {
                  val str: String = new String(recourd.event.getBody.array())
                  str.split("\\|")
                })
              val falseInfo = mapResult.filter(line => line.length < 38)
              val filter = mapResult.filter(line => line.length >= 38)
              val saEntity = filter.map(lines => {
                CacheSourceTools.getSACacheInfo(lines: Array[String], ipRulesBroadcast, ipwebBroadcast)
              })

              val filterMap: RDD[Array[String]] =filter
                .filter(lines => {
                  lines(11) != "NULL" && lines(11) != ""
                })
                .filter(lines => {
                  lines(35) != "NULL" && lines(34) != "0" && lines(34) != ""
                })
                .filter(lines => {
                  lines(14) != "NULL" && lines(14) != "0" && lines(14) != ""
                })
              //五分钟报表
              val trueInfo= filterMap.map(info => {
                  CacheSourceTools.getCacheSourceEntity(1, info(2), info(21), info(6), info(12), info(4), info(14), info(34), info(11), info(27), info(29), info(30), info(16), info(15), info(8), info(1), info(35), info(36), info(37), info(17), ipRulesBroadcast, ipwebBroadcast, info(28),"")
              })
              val inputDF = trueInfo.toDF()
              inputDF.createOrReplaceTempView("caches")
              val result = spark.sql("select flowUnit,all_flow,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time,cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain,business_time,count(1) as all_num,sum(req_ok_num) as req_ok_numb,(count(1)-sum(req_ok_num))as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow,sum(miss_flow) as miss_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num,sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed from caches group by cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain ,flowUnit,all_flow,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time ")
              //EsSparkSQL.saveToEs(result, "ifp_cache_" + CacheSourceTools.getNowDate() + "_flume/TB_DW_Cache_Overview_5m")
              result.show()
              //20171218
              val dateDay = CacheSourceTools.getNowDate()
              //201712181715
              val justmm = getDate(CacheSourceTools.getFiveDate(CacheSourceTools.getNowDateJustmm()))
              if(result.count()>0){
//                result.write.parquet("/ifp_source/DataCenter/sa/TB_SA_CACHE/"+CacheSourceTools.getNowDate()+"/small/"+CacheSourceTools.getNowDateJustmm()+"/data/correct_store/330000")
              }
              val saDF = saEntity.toDF()
              //数据写入hdfs
              if(saDF.count()>0)
              {
//                saEntity.toDF().write.parquet("G:\\atest/ifp_source/DataCenter/sa/TB_SA_CACHE/"+CacheSourceTools.getNowDate()+"/small/"+justmm+"/data/correct_store/330000")
                //saDF.write.parquet("G:\\atest/ifp_source/DataCenter/sa/TB_SA_CACHE/330000/"+dateDay+"/small/"+justmm+"/data/correct_store")
              }
              //脏数据存储路径
              val falseDF = falseInfo.toDF()
              println(falseDF.count())
              if(falseDF.count()>0){
                //falseDF.write.parquet("G:\\atest/ifp_source/DataCenter/sa/TB_SA_CACHE/330000/"+dateDay+"/small/"+justmm+"/data/error_store")
//                falseInfo.toDF().write.parquet("/ifp_source/DataCenter/sa/TB_SA_CACHE/"+CacheSourceTools.getNowDate()+"/small/"+justmm+"/data/error_store/330000")
              }
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
        case e: Exception => "本次写入失败"+CacheSourceTools.getNowDateJustmm()
      }
    }
    */
    ssc.start()
    ssc.awaitTermination()





  }

  def getDate(str:String): String ={
    val dayTime: String = str.substring(0, str.length - 4)
    val mmTime = str.substring(str.length-4,str.length)
    println("天:"+dayTime+"分"+mmTime)
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
