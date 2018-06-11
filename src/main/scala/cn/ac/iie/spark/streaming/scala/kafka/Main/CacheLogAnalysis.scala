package cn.ac.iie.spark.streaming.scala.kafka.Main

import cn.ac.iie.spark.streaming.scala.base.CacheOverview
import cn.ac.iie.spark.streaming.scala.kafka.GetTimeUtils
import cn.ac.iie.spark.streaming.scala.kafka.Impl.SmallFileAnalysis
import cn.ac.iie.spark.streaming.scala.kafka.Service.StreamingContextService
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import cn.ac.iie.spark.streaming.util.{CacheSourceTools, IpTools, SparkSessionSingleton}
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CacheLogAnalysis {

    def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

      if(args.length!=2){
        println("请输入参数")
        System.exit(0)
      }
      if(args(0)=="Streaming"){
        if(args(1)=="small"){
          SmallFileAnalysis.OverrideToEsByFlume()
        }else if(args(1)=="big"){

        }

      }
      SmallFileAnalysis.OverrideToEsByFlume()


  }



}
