package cn.ac.iie.spark.streaming.scala.hdfs

import java.util.regex.Pattern

import cn.ac.iie.spark.streaming.scala.base.CacheOverview
import cn.ac.iie.spark.streaming.util.{CacheSourceTools, IpTools, SparkSessionSingleton}
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.{ArrayBuffer, HashMap}

object OverviewReadFileToES {

  def main(args: Array[String]): Unit = {
    val properties = CacheSourceTools.loadProperties()
    @transient
    val sparkConf = new SparkConf().setAppName("fromHdfsOverview")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("es.index.auto.create", "true")
    sparkConf.set("user.name", properties.getProperty("user.name"))
//    sparkConf.setMaster("spark://10.0.30.101:7077")
        sparkConf.setMaster("local[3]")
//    sparkConf.set("es.nodes", properties.getProperty("es.nodes"))
//    sparkConf.set("es.port", properties.getProperty("es.port"))

    val context: SparkContext = new SparkContext(sparkConf)
    val sql = new SQLContext(context)
    val spark = SparkSessionSingleton.getInstance(sparkConf)
    import spark.implicits._
    val ipwebinfoArray: HashMap[String, (String, String, String, String, String, String)] = getWebSiteInfo
    //WEB Host匹配规则
    @transient
    val ipwebBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]] = context.broadcast(ipwebinfoArray)
    //IP规则
    val ipRulesArray = IpTools.getIPControl()
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = context.broadcast(ipRulesArray)

    println(properties.getProperty("kafka.topics"))


    val file = context.textFile("G:\\atest\\stdaccess.log")
    val map: RDD[CacheOverview] = file.map(_.split("' ")).filter(_.length == 38)
      .filter(lines => {
        lines(34).replaceAll("'","").toLowerCase=="null"||lines(14).replaceAll("'","").toLowerCase=="null"||lines(34).replaceAll("'","")==""||lines(14).replaceAll("'","")==""||StringUtils.isNumeric(lines(14).replaceAll("'",""))  == true && StringUtils.isNumeric(lines(34).replaceAll("'",""))  == true //&&Pattern.matches("\\d+\\.\\d+\\.\\d+\\.\\d+",lines(3))//lines(34) != "0" && lines(34) != ""&& &&lines(35) != "NULL" &&  lines(37) != "NULL"&& lines(11)!="" &&   lines(14) != "NULL" && lines(14) != "0" && lines(14) != ""
      })
      .map(info => {
        CacheSourceTools.getCacheSourceEntity(1, info(2).replaceAll("'",""), info(21).replaceAll("'",""), info(6).replaceAll("'",""), info(12).replaceAll("'",""), info(4).replaceAll("'",""), info(14).replaceAll("'",""), info(34).replaceAll("'",""), info(11).replaceAll("'",""), info(27).replaceAll("'",""), info(29).replaceAll("'",""), info(30).replaceAll("'",""), info(16).replaceAll("'",""), info(15).replaceAll("'",""), info(8).replaceAll("'",""), info(1).replaceAll("'",""), info(35).replaceAll("'",""), info(36).replaceAll("'",""), info(37).replaceAll("'",""), info(17).replaceAll("'",""), ipRulesBroadcast, ipwebBroadcast, info(28).replaceAll("'",""), info(25).replaceAll("'",""), "")
      })
    println(1111)

    val dataFrame = map.toDF()
    println(dataFrame.count())
    println(dataFrame.show())

  }

}
