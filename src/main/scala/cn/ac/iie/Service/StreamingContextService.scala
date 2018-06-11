package cn.ac.iie.Service

import cn.ac.iie.spark.streaming.scala.kafka.Interface.StreamingContextInterface
import cn.ac.iie.spark.streaming.util.{CacheSourceTools, IpTools, SparkSessionSingleton}
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

object StreamingContextService extends StreamingContextInterface{

//  var ipwebinfoArray: HashMap[String, (String, String, String, String, String, String)] = getWebSiteInfo
//  var ipwebBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]] =null
//  //IP规则
//  val ipRulesArray = IpTools.getIPControl()
//  var ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = null

  override def createHDFSContext(checkpointDirectory:String): StreamingContext = {

    val conf = new SparkConf()
      .setAppName("HDFSInputData")
      //      .setMaster("spark://master:7077")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkpointDirectory)
//    ipwebBroadcast= ssc.sparkContext.broadcast(ipwebinfoArray)
//    ipRulesBroadcast = ssc.sparkContext.broadcast(ipRulesArray)
    ssc
  }

  override def createFlumeReceiverContext(conf:SparkConf): StreamingContext = {
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(60))
    ssc
  }

  override def createSparkConf(appName:String): SparkConf = {
    val properties = CacheSourceTools.loadProperties()
    val conf = new SparkConf().setAppName(appName)
//      .setMaster(properties.getProperty("spark.master"))
            .setMaster("local[*]")
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("spark.executor.memory", "5g")
      .set("spark.cores.max", properties.getProperty("runtime.cores.max"))
      .set("es.port", properties.getProperty("es.port"))
      .set("user.name", properties.getProperty("user.name"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("hive.metastore.warehouse.dir","hdfs://10.0.30.101:8020/user/hive/warehouse")
      .set("javax.jdo.option.ConnectionURL","jdbc:mysql://10.0.30.12:3306/hivemetastoredb?useUnicode=true&amp;characterEncoding=UTF-8")
      .set("hive.metastore.uris","thrift://10.0.30.101:9083")
    conf
  }

  def createSparkSession(AppName:String):SparkSession={
    val spark=SparkSession.builder()
      .appName(AppName)
      .master("yarn")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://10.0.30.101:9000/user/hive/warehouse")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "10.0.30.101,10.0.30.102,10.0.30.105,10.0.30.107")
      .config("es.port", "9200")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

}
