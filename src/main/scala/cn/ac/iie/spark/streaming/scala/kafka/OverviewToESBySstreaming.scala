package cn.ac.iie.spark.streaming.scala.kafka

import java.text.{DecimalFormat, ParseException, SimpleDateFormat}
import java.util.{ArrayList, Calendar, Date}

import cn.ac.iie.spark.streaming.scala.base.CacheOverview
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import cn.ac.iie.spark.streaming.util.{CacheSourceTools, IpTools, SparkSessionSingleton, WebSiteInfo}
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object OverviewToESBySstreaming {

  def main(args: Array[String]): Unit = {
    val properties = CacheSourceTools.loadProperties()
    // 解析配置文件，获取程序参数
    val conf = new SparkConf().setAppName("OverviewToES")
    .setMaster("local[2]")
    //.setMaster("spark://10.0.30.101:7077")
    conf.set("user.name",properties.getProperty("user.name"))
    conf.set("es.nodes",properties.getProperty("es.nodes"))
    conf.set("es.port", properties.getProperty("es.port"))
    conf.set("es.index.auto.create", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val topics = Set(properties.getProperty("kafka.topics"))
    val brokers = properties.getProperty("kafka.brokers")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    // kafka参数
    val kafkaParams = Map[String, String](
      //"bootstrap.servers" -> brokers,
      "metadata.broker.list"->brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "cachetest"
      //"auto.offset.reset" -> "smallest"
    )//val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,topics)
    val spark = SparkSessionSingleton.getInstance(conf)
    import spark.implicits._
    var dataFrameDataStatus = false
    var overview: CacheOverview = null
    var allDF: DataFrame = null
    //缓存数据集
    var overviewList: mutable.MutableList[CacheOverview] = mutable.MutableList[CacheOverview]()
    //广播变量
    @transient
    val ipwebinfoArray: mutable.HashMap[String, (String, String, String, String, String, String)] = getWebSiteInfo
    //WEB Host匹配规则
    @transient
    val ipwebBroadcast : Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] =ssc.sparkContext.broadcast(ipwebinfoArray)
    //IP规则
    var ipRulesArray= IpTools.getIPControl()
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(IpTools.getIPControl())
    //val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(ipRulesArray)
    //唯一标示列
    var rowID=0
    //解析出其它信息并封装
    dstream.foreachRDD(
      lines => {
        //println(lines.count)
        lines.toDF().show()
        if(!lines.isEmpty()){
          //手动维护偏移量(当前不是手动维护偏移量)
          //val offsetRanges = lines.asInstanceOf[HasOffsetRanges].offsetRanges
            lines.map(lines=>{
            lines._2.toString.split("\\|")
            //println("100："+split.length)
            //split
          }).filter(line=>line.length>=37)
            //.filter(lines=>{lines(37)!="NULL"})
            //.filter(lines=>{lines(35)!="NULL"})
            .map(
            info => {
              val entity = CacheSourceTools.getCacheSourceEntity(rowID,info(2),info(21),info(6),info(12),info(4),info(14),info(34),info(11),info(27),info(29),info(30),info(16),info(15),info(8),info(1), info(36),info(35),info(37),info(17),ipRulesBroadcast,ipwebBroadcast,info(28),info(25),"")
              val df: DataFrame = Seq(entity).toDF()
              EsSparkSQL.saveToEs(df, "ifp_cache_" + CacheSourceTools.getNowDate() + "_es/TB_DW_Cache_Overview_5m")
              //println(overviewList.size)
              //根据不同的批次写入ES
/*              if(overviewList.size!=0&&entity.bussiness_time!=overviewList(overviewList.size-1).bussiness_time){
                  var batchDF: DataFrame = null
                  overviewList.foreach(list => {
                    if (batchDF == null) {
                      batchDF = Seq(list).toDF()
                    } else {
                      val f = Seq(list).toDF()
                      batchDF = allDF.union(f)
                    }
                  })
                  val joinBatch: DataFrame = CacheSourceTools.getResultDataFrame(batchDF)
                  joinBatch.show()
                  //joinBatch.saveToEs("ifp_cache_" + CacheSourceTools.getNowDate() + "_small/TB_DW_Cache_Overview_5m")
                  joinBatch.saveToEs("ifp_cache_20171207_small/TB_DW_Cache_Overview_5m")
                  //EsSparkSQL.saveToEs(joinBatch, "ifp_cache_" + CacheSourceTools.getNowDate() + "_small/TB_DW_Cache_Overview_5m")
                  overviewList.clear()
                overviewList += entity
                }else {
                overviewList += entity
              }*/
            }
          )
        }
        //println("001:"+overviewList.size)
      }

    )
/*    if (overviewList.size != 0) {
      overviewList.foreach(list => {
        if (allDF == null) {
          allDF = Seq(list).toDF()
        } else {
          val f = Seq(list).toDF()
          allDF = allDF.union(f)
        }
      })
      val join: DataFrame = CacheSourceTools.getResultDataFrame(allDF)
      EsSparkSQL.saveToEs(join, "ifp_cache_" + CacheSourceTools.getNowDate() + "_small/_mapping/TB_DW_Cache_Overview_5m")
      //      allDF=null
      //      overviewList=null
    }*/
    ssc.start()
    ssc.awaitTermination()
  }


}
