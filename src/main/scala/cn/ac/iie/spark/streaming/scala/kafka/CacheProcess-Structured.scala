package cn.ac.iie.spark.streaming.scala.kafka

import org.apache.spark.sql.SparkSession

import cn.ac.iie.spark.streaming.util._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset

case class CacheLogBean_(date: String, deviceIp: String, userIp: String, serverIp: String, httpMethod: String, httpVersion: String, host: String, uri: String, userAgent: String, referer: String, contentType: String, statusCode: String, cacheState: String, cacheHost: String, cacheFlow: String, requestStartDate: String, requestEndDate: String, firstResponseTime: String, URLHashCode: String, cacheType: String, backSourceIP: String, backTargetIP: String, backHttpMethod: String, backHttpVersion: String, backHost: String, backUri: String, backContentType: String, backState: String, backCloseState: String, backCacheControl: String, backMaxGge: String, backFileSize: String, backSaveState: String, backServerHost: String, backFlow: Long, backRequestStartDate: String, backRequestEndDate: String, backFirstResponseTime: String)

object CacheProcess {
  def main(args: Array[String]) {
    val topics = "cachetest";
    val brokers = "node03:9092,node04:9092"
    //    val sparkConf = new SparkConf().setAppName("CacheProcess").setMaster("local[2]")
    //    sparkConf.set("es.index.auto.create", "true")
    //    sparkConf.set("es.nodes", "10.0.30.101,10.0.30.102,10.0.30.105,10.0.30.107")
    //    sparkConf.set("es.port", "9200")
    val spark = SparkSession.builder.appName("CacheProcess").config(new SparkConf().setAppName("CacheProcess").setMaster("local[2]")).getOrCreate()
    import spark.implicits._
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", topics).load()
    // Split the lines into words  
    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
      .writeStream
      .format("elasticsearch")
      .option("es.index.auto.create", "true")
      .option("es.nodes", "10.0.30.101,10.0.30.102,10.0.30.105,10.0.30.107")
      .option("es.port", "9200")
      .start()
    // Generate running word count  
    query.awaitTermination()
  }
}