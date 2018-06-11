package cn.ac.iie.spark.streaming.scala.hdfs

import cn.ac.iie.spark.streaming.util.IpTools
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import cn.ac.iie.spark.streaming.util.CacheSourceTools
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import cn.ac.iie.spark.streaming.util.SparkSessionSingleton
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import cn.ac.iie.spark.streaming.scala.base.CacheOverview
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import org.apache.spark.sql.{ DataFrame, Dataset, Row, SQLContext }
import org.elasticsearch.spark.sql._
import org.apache.spark.streaming.dstream.InputDStream

object FromHDFSOverview extends Serializable {

  def main(args: Array[String]): Unit = {

    val properties = CacheSourceTools.loadProperties()
    @transient
    val sparkConf = new SparkConf().setAppName("fromHdfsOverview").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("es.index.auto.create", "true")
    sparkConf.set("user.name", properties.getProperty("user.name"))
    sparkConf.setMaster("spark://10.0.30.101:7077")
    //    sparkConf.setMaster("local[4]")
    sparkConf.set("es.nodes", properties.getProperty("es.nodes"))
    sparkConf.set("es.port", properties.getProperty("es.port"))
    @transient
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Seconds(180))
    val spark = SparkSessionSingleton.getInstance(sparkConf)
    import spark.implicits._
    val dstream = ssc.textFileStream("/user/hadoop-user/coll_data/cache_log/jiangxi/791/20171025/")
    var rowID = 0
    //广播变量
    @transient
    val ipwebinfoArray: HashMap[String, (String, String, String, String, String, String)] = getWebSiteInfo
    //WEB Host匹配规则
    @transient
    val ipwebBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]] = sparkContext.broadcast(ipwebinfoArray)
    //IP规则
    val ipRulesArray = IpTools.getIPControl()
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = sparkContext.broadcast(ipRulesArray)
    dstream.print()
    val logs = dstream.map(_.split("\\|"))
      .filter(_.length == 38)
      .filter(_(37) != "NULL")
      .filter(_(35) != "NULL")
      .map(info => {
          CacheSourceTools.getCacheSourceEntity(1, info(2), info(21), info(6), info(12), info(4), info(14), info(34), info(11), info(27), info(29), info(30), info(16), info(15), info(8), info(1), info(35), info(36), info(37), info(17), ipRulesBroadcast, ipwebBroadcast,info(28),info(25),"")
                 })
    logs.foreachRDD(log => {
      val joinBatch: DataFrame = CacheSourceTools.getResultDataFrame(log.toDF())
      //      joinBatch.show()
      joinBatch.saveToEs("ifp_cache_" + CacheSourceTools.getNowDate() + "_small/TB_DW_Cache_Overview_5m")
    })
    ssc.start()
    ssc.awaitTermination()

  }
}