package cn.ac.iie.spark.streaming.scala.kafka

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import cn.ac.iie.spark.streaming.scala.StreamingExamples
import cn.ac.iie.spark.streaming.util._
import kafka.serializer.StringDecoder

case class LogDataBean(word: String, num: Int)
//case class CacheLogBean(date:String,deviceIp:String,userIp:String,serverIp:String,httpMethod:String,httpVersion:String,host:String,uri:String,userAgent:String,referer:String,contentType:String,statusCode:String,cacheState:String,cacheHost:String,cacheFlow:String,requestStartDate:String,requestEndDate:String,firstResponseTime:String,URLHashCode:String,cacheType:String,backSourceIP:String,backTargetIP:String,backHttpMethod:String,backHttpVersion:String,backHost:String,backUri:String,backContentType:String,backState:String,backCloseState:String,backCacheControl:String,backMaxGge:String,backFileSize:String,backSaveState:String,backServerHost:String,backFlow:Long,backRequestStartDate:String,backRequestEndDate:String,backFirstResponseTime:String)
case class Record(word: String)
object DirectKafkaWordCount {
  def main(args: Array[String]) {
    //    if (args.length < 2) {
    //      System.err.println(s"""
    //        |Usage: DirectKafkaWordCount <brokers> <topics>
    //        |  <brokers> is a list of one or more Kafka brokers
    //        |  <topics> is a list of one or more kafka topics to consume from
    //        |
    //        """.stripMargin)
    //      System.exit(1)
    //    }

    StreamingExamples.setStreamingLogLevels()
    //    val Array(brokers, topics) = args
    val topics = "cachetest";
    val brokers = "node03:9092,node04:9092"
    // Create context with 2 second batch interval
    //    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("spark://10.0.30.101:7077")
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "10.0.30.101,10.0.30.102,10.0.30.105,10.0.30.107")
    sparkConf.set("es.port", "9200")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(30))
    val sqlContext = new SQLContext(sc)
    //      ssc.checkpoint("");
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and save to ES
   /*     val lines = messages.map(_._2)
    val words = lines.flatMap(_.split("\\|"))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map(line => {
      val word = line._1
      val num = line._2
      LogDataBean(word, num)
    })
    wordCounts.saveToEs("spark/newWordCount")*/
    //change sparkSql
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split("\\|"))
    words.foreachRDD { rdd =>
      // Get the singleton instance of SparkSession
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w.replaceAll("(\0|\\s*|\r|\n)", ""))).toDF()
      // Create a temporary view
      wordsDataFrame.createOrReplaceTempView("words")
      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
      println(s"=========" + new Date(System.currentTimeMillis()) + "=========")
      wordCountsDataFrame.show()
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

