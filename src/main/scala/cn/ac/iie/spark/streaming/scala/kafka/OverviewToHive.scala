/**
  * creator:SingTaoChi
  * time:2017/11/28
  * effect:实时处理数据，并写入Hive
  */

package cn.ac.iie.spark.streaming.scala.kafka

import java.text.SimpleDateFormat
import java.util.Date

import cn.ac.iie.spark.streaming.scala.base.TBDWCacheOverview
import cn.ac.iie.spark.streaming.util.{CodeTools, DateTools, IntParam}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OverviewToHive {

  def main(args: Array[String]): Unit = {

    // 解析配置文件，获取程序参数
    //val load = ConfigFactory.load()
    // kafka集群节点
    //val brokers = load.getString("kafka.brokers")
    // 数据所在主题
    //val topics = load.getString("kafka.topics")
    // 消费组
    //val consumer = load.getString("kafka.consumer")

    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("Overview")
    val sc: SparkContext = new SparkContext()
    val hiveContext: HiveContext = new HiveContext(sc)

    import hiveContext.implicits._
    hiveContext.sql("use ifp_caches")

    //间隔2秒
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(2))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val CacheOverview: DStream[TBDWCacheOverview] = lines.map({
      line =>
        val strings: Array[String] = line.split("|")
        //日志写入时间
        val logintime = DateTools.getFiveTime(strings(0).toString)
        //缓存设备信息:缓存服务器IP地址
        val device_IP = strings(1).toString
        //HTTP请求的Method
        val method = strings(4).toString
        //HTTP请求的Version
        val version = strings(5).toString
        //HTTP请求的Host
        val host: String = strings(6).toString
        //user agent
        val agent: String = strings(8).toString
        //HTTP Status-code
        val statusCode: String = CodeTools.getCodeSummary(strings(11).toString)
        //缓存命中状态
        val cacheState = CodeTools.getCacheState(strings(12).toString)
        //缓存吐出流量
        val flow: BigInt = unapply(strings(14).toString)
        //首字节响应时间 17
        val dateFormation: Date = dataTransFormation(strings(8).toString)
        //缓存命中类型  19
        val cacheHitType: String = strings(19).toString
        //回源method  22
        val httpMethod: String = strings(22).toString
        //回源version 23
        val httpVersion: String = strings(23).toString
        //回源Host 24
        val httpHost: String = strings(24).toString
        //回源HTTP Status Code 27
        val code: String = StatusCode(strings(27).toString)
        //回源关闭状态 28
        val closeStatus: String = strings(28).toString
        //回源流量 34
        val flowTraffic: BigInt = sourceTrafficConversion(strings(34).toString)
        //回源请求时间 35 36
        val timeValue: Long = DateTools.getTimeValue(strings(35).toString, strings(36).toString)
        //源站首字节响应时间 37
        val responseTime: String = strings(37).toString
        TBDWCacheOverview(
          logintime,
          device_IP,
          method,
          version,
          host,
          agent,
          statusCode,
          cacheState,
          flow,
          dateFormation,
          cacheHitType,
          httpMethod,
          httpVersion,
          httpHost,
          code,
          closeStatus,
          flowTraffic,
          timeValue,
          responseTime)
    })

    CacheOverview.foreachRDD(
      lines=>{

      }

    )



    ssc.start()
    ssc.awaitTermination()

  }

  //首字节响应时间
  def dataTransFormation(data:String): Date ={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parseDate: Date = dateFormat.parse(data)
    parseDate
  }

  //回源StatusCode
  def StatusCode(code:String): String ={
    val summary = code match {
      case code if code.startsWith("2") => "2xx"
      case code if code.startsWith("3") => "3xx"
      case code if code.startsWith("4") => "4xx"
      case code if code.startsWith("5") => "5xx"
      case _                            => "other"
    }
    summary
  }

  //回源流量转换
  def sourceTrafficConversion(flow:String): BigInt ={
    try {
      flow.toInt
    } catch {
      case e: NumberFormatException => -1
    }
  }

  //吞吐流量
  def unapply(flow:String): BigInt ={
    try {
      flow.toInt
    } catch {
      case e: NumberFormatException => -1
    }
  }


}
