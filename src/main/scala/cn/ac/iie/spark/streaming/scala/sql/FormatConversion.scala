package cn.ac.iie.spark.streaming.scala.sql

import cn.ac.iie.spark.streaming.util.{CacheSourceTools, IpTools}
import cn.ac.iie.spark.streaming.util.CacheSourceTools.getDateWithInputTime
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

case class Sourcelog(date: String, deviceIp: String, userIp: String, serverIp: String, httpMethod: String, httpVersion: String, host: String, uri: String, userAgent: String, referer: String, contentType: String, statusCode: String, cacheState: String, cacheHost: String, cacheFlow: String, requestStartDate: String, requestEndDate: String, firstResponseTime: String, URLHashCode: String, cacheType: String, backSourceIP: String, backTargetIP: String, backHttpMethod: String, backHttpVersion: String, backHost: String, backUri: String, backContentType: String, backStateCode: String, backCloseState: String, backCacheControl: String, backMaxGge: String, backFileSize: String, backSaveState: String, backServerPort: String, backFlow: String, backRequestStartDate: String, backRequestEndDate: String, backFirstResponseTime: String)

object FormatConversion {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ConvertParquet").setMaster("yarn")
//        val conf = new SparkConf().setAppName("ConvertParquet").setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("user.name", "hadoop-user")
    conf.set("user", "hadoop-user")
    conf.set("spark.sql.parquet.compression.codec", "snappy")
//    conf.set("spark.sql.parquet.compression.codec", "gzip")
    val sc: SparkContext = new SparkContext(conf)

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._
    //读取数据
    val files = spark.read.textFile("hdfs://10.0.30.101:8020/user/hadoop-user/coll_data/cache_log/jiangxi/791/20170806/big")
    //0806---0404
    //0805---0403
    println(files.map(_.split("\\|")).filter(_.length >= 38).count())   //.map(file => getInfo(file))

    val cacheData = files.map(_.split("\\|")).filter(_.length >= 38).map(file => Sourcelog(
      file(0).replaceAll("2017/08/06","2018/04/06"),
//      getDateWithInputTime,
      file(1),
      file(2),
      file(3),
      file(4),
      file(5),
      file(6),
      file(7),
      file(8),
      file(9),
      file(10),
      file(11),
      file(12),
      file(13),
      file(14),
      file(15),
      file(16),
      file(17),
      file(18),
      file(19),
      file(20),
      file(21),
      file(22),
      file(23),
      file(24),
      file(25),
      file(26),
      file(27),
      file(28),
      file(29),
      file(30),
      file(31),
      file(32),
      file(33),
      file(34),
      file(35),
      file(36),
      file(37)))

    val CachedataFrame = cacheData.toDF()

    CachedataFrame.repartition(3).write.orc("hdfs://10.0.30.101:8020/ifp_source/DataCenter/sa/big/orc_snap/20180406")




    //    println(CachedataFrame.count())
//    CachedataFrame.write.parquet("hdfs://10.0.30.101:8020/ifp_source/DataCenter/sa/TB_R_W_CACHE/20170806/big/2017120415/data/store")
//    CachedataFrame.saveAsTextFile("codec/snappy",classOf[Sourcelog])
//    CachedataFrame.saveAsTextFile("codec/gzip",classOf[Sourcelog])
    sc.stop()

  }

  def getInfo(lines:Array[String]): Unit ={

/*    val ipwebinfoArray= getWebSiteInfo
    val ipwebBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(ipwebinfoArray)
    //IP规则
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(IpTools.getIPControl())
    val df = CacheSourceTools.getSACacheInfo(lines,"huawei", ipRulesBroadcast, ipwebBroadcast).toDF()
    df.write.repartition(3).orc("hdfs://10.0.30.101:8020/ifp_source/DataCenter/sa/big/orc_realgzip")
    df.saveAsTextFile("codec/gzip",classOf[Sourcelog])*/
  }

}