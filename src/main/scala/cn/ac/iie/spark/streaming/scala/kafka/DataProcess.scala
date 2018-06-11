package cn.ac.iie.spark.streaming.scala.kafka
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.spark.sql.sparkDataFrameFunctions
import org.elasticsearch.spark.streaming._
import cn.ac.iie.spark.streaming.java.GetDomainInfo
import cn.ac.iie.spark.streaming.java.GetFileInfo
import cn.ac.iie.spark.streaming.scala.StreamingExamples
import cn.ac.iie.spark.streaming.scala.base.CacheAccessBean
import cn.ac.iie.spark.streaming.scala.base.CacheAccessLog
import cn.ac.iie.spark.streaming.util.DateTools
import cn.ac.iie.spark.streaming.util.IpTools
import cn.ac.iie.spark.streaming.util.SparkSessionSingleton
import cn.ac.iie.spark.streaming.util.WebSiteInfo

import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder

//case class CacheLogBean(fileType: String, date: Long, deviceIp: String, userIp: String, serverIp: String, city: String, operator: String, system: String, userType: String, httpMethod: String, httpVersion: String, host: String, mainDomain: String, uri: String, urlType: String, userAgent: String, TerminalType: String, referer: String, contentType: String, statusCode: String, codeSummary: String, cacheState: String, stateSummary: String, cacheHost: String, cacheFlow: String, requestStartDate: String, requestEndDate: String, CacheTimeValue: Long, firstResponseTime: Long, URLHashCode: String, cacheType: String, backSourceIP: String, backTargetIP: String, backHttpMethod: String, backHttpVersion: String, backHost: String, backUri: String, backContentType: String, backStateCode: String, backCodeSummary: String, backCloseState: String, backCacheControl: String, backMaxGge: String, backFileSize: String, backSaveState: String, backServerPort: String, backFlow: Long, backRequestStartDate: String, backRequestEndDate: String, backTimeValue: Long, backFirstResponseTime: Long, requestNum: Long)
//case class CacheLogBean(fileType: String, date: Long, deviceIp: String, userIp: String, serverIp: String, belong: (String, String, String, String, String, String, String), httpMethod: String, httpVersion: String, host: String, mainDomain: String, uri: String, urlType: String, userAgent: String, TerminalType: String, referer: String, contentType: String, statusCode: String, codeSummary: String, cacheState: String, stateSummary: String, cacheHost: String, cacheFlow: String, requestStartDate: String, requestEndDate: String, CacheTimeValue: Long, firstResponseTime: Long, URLHashCode: String, cacheType: String, backSourceIP: String, backTargetIP: String, backHttpMethod: String, backHttpVersion: String, backHost: String, backUri: String, backContentType: String, backStateCode: String, backCodeSummary: String, backCloseState: String, backCacheControl: String, backMaxGge: String, backFileSize: String, backSaveState: String, backServerPort: String, backFlow: Long, backRequestStartDate: String, backRequestEndDate: String, backTimeValue: Long, backFirstResponseTime: Long, requestNum: Long)
//case class CacheLogBean(fileType: String, date: Long, deviceIp: String, userIp: String, serverIp: String, userType: String, city: String, operator: String, system: String, httpMethod: String, httpVersion: String, host: String, mainDomain: String, uri: String, urlType: String, userAgent: String, TerminalType: String, referer: String, contentType: String, statusCode: String, codeSummary: String, cacheState: String, stateSummary: String, cacheHost: String, cacheFlow: String, requestStartDate: String, requestEndDate: String, CacheTimeValue: Long, firstResponseTime: Long, URLHashCode: String, cacheType: String, backSourceIP: String, backTargetIP: String, backHttpMethod: String, backHttpVersion: String, backHost: String, backUri: String, backContentType: String, backStateCode: String, backCodeSummary: String, backCloseState: String, backCacheControl: String, backMaxGge: String, backFileSize: String, backSaveState: String, backServerPort: String, backFlow: Long, backRequestStartDate: String, backRequestEndDate: String, backTimeValue: Long, backFirstResponseTime: Long, requestNum: Long)

object DataProcess {
  val nowDate = DateTools.getNowDate()
  StreamingExamples.setStreamingLogLevels()
  //构建Map  
  def setFromOffsets(list: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (offset <- list) {
      val tp = TopicAndPartition(offset._1, offset._2) //topic和分区数  
      fromOffsets += (tp -> offset._3) // offset位置  
    }
    fromOffsets
  }
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    //==================================================
    sparkConf.setAppName("DataProcess")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    sparkConf.set("spark.shuffle.consolidateFiles", "true")
    sparkConf.set("park.speculation", "true")
    //    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
    sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
    sparkConf.setExecutorEnv("SPARK_DAEMON_JAVA_OPTS", " -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
    sparkConf.registerKryoClasses(Array(classOf[cn.ac.iie.spark.streaming.scala.base.CacheAccessBean], classOf[cn.ac.iie.spark.streaming.java.GetDomainInfo], classOf[cn.ac.iie.spark.streaming.java.GetFileInfo]))

    //    sparkConf.set("num-executors", "9")
    //    sparkConf.set("executor-cores", "4")
    //    sparkConf.set("executor-memory", "39G")
    //    sparkConf.set("driver-memory", "10G")
    sparkConf.set("spark.defalut.parallelism", "10")
    //==================================================
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.executor.cores", "2")
    sparkConf.set("spark.driver.memory", "2G")
    sparkConf.set("spark.driver.memory", "2G")
    //        sparkConf.set("spark.defalut.parallelism", "4")

    //    sparkConf.set("spark.executor.memory", "30G")
    //    sparkConf.set("spark.executor.cores", "2")
    //    sparkConf.set("spark.driver.memory", "10G")
    //           sparkConf.setMaster("yarn")
    sparkConf.setMaster("local[2]")
    //    sparkConf.setMaster("spark://10.0.30.101:7077")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "10.0.30.101,10.0.30.102,10.0.30.105,10.0.30.107")
    sparkConf.set("es.port", "9200")
    //  sparkConf.set("es.mapping.date.rich", "false")
    //  sparkConf.set("date_detection", "false")
    //    sparkConf.set("spark.sql.shuffle.partitions", "20")
    sparkConf.set("pushdown", "true")
    //==================================================
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Seconds(60))

    //    val ipRulesArray = IpTools.getIPControl()
    //    val ipRulesBroadcast = sparkContext.broadcast(ipRulesArray)
    //    val WebinfoArray = WebSiteInfo.getWebSiteInfo
    //    val webSiteBroadcast = sparkContext.broadcast(WebinfoArray)

    //  sparkContext.setLogLevel("DEBUG")
    val topics = "cachetest";
    val topicsSet = topics.split(",").toSet
    val brokerList = "node03:9092,node04:9092"
    val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> brokerList)
    /*  val zkHost = "node03:2181,node04:2181"
    val zkClient = new ZkClient(zkHost)
    var kafkaStream: InputDStream[(String, String)] = null
    var offsetRanges = Array[OffsetRange]()
    //    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> brokerList, "zookeeper.connect" -> zkHost)
    val topicDirs = new ZKGroupTopicDirs("TEST_TOPIC_spark_streaming_testid", topics) //创建一个 ZKGroupTopicDirs 对象，对保存
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}") //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    var fromOffsets: Map[TopicAndPartition, Long] = Map() //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
//        val offsetList:List[(String, Int, Long)] = List((topics, 0, 0),(topics, 1, 0),(topics, 2, 0),(topics, 3, 0)) //指定topic，partition_no，offset  
//        val fromOffsets = setFromOffsets(offsetList)     //构建参数 
    if (children > 0) { //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topics, i)
        fromOffsets.+(tp -> partitionOffset.toLong) //将不同 partition 对应的 offset 增加到 fromOffsets 中
      }
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    }
    val logbean=kafkaStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset 
      rdd
    }.map(_._2).persist().filter(CacheAccessLog.isValidateLogLine).map(
      line => {
        //        CacheAccessLog.parseLogLine(line, ipRulesBroadcast, webSiteBroadcast)
        CacheAccessLog.parseLogLine(line)

      })
    */
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //    val messages= KafkaUtils.createDirectStream(ssc, kafkaParams, fromOffsets, messageHandler)  
    //      messages.repartition(4)
    // DomainInfo
    //    val domaininfo = new GetDomainInfo();
    //    val fileinfo = new GetFileInfo();
    val logs = messages.map(_._2).filter(CacheAccessLog.isValidateLogLine).persist()
    val logbean = logs.filter(CacheAccessLog.isValidateLogLine).map(
      line => {
        //        CacheAccessLog.parseLogLine(line, ipRulesBroadcast, webSiteBroadcast)
        CacheAccessLog.parseLogLine(line)

      })
    //    logs.print()

    /*    val logbean = logs.mapPartitions { line =>
      {
        var result: List[CacheAccessBean] = List[CacheAccessBean]()
        while (line.hasNext) {
          if (CacheAccessLog.isValidateLogLine(line.next())) {
            var cachelog = CacheAccessLog.parseLogLine(line.next(), ipRulesBroadcast, webSiteBroadcast)
            result.+:(cachelog)
//            println("sssssssssssss=======" + cachelog.date)
          }
        }
        result.iterator
      }

    }*/

    //    val logs = logbean.filter(log => domaininfo.validDomain(log.host)).filter(log => log.date != null)
    println(s"0000000==" + new Date(System.currentTimeMillis()) + "=========" + nowDate + " =====")
    //    ipRulesBroadcast.unpersist()
    //    webSiteBroadcast.unpersist()
    logbean.foreachRDD { rdd =>
      /*         for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
      }*/
      println(s"22222222==" + new Date(System.currentTimeMillis()) + "=========" + nowDate + "=============" + rdd.count())
      //      rdd.persist()
      //      val logbean=rdd.mapPartitions(f, preservesPartitioning)

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      //      val logDataFrame = rdd.map(bean => CacheLogBean(bean.fileType, bean.date, bean.deviceIp, bean.userIp, bean.serverIp, IpTools.getIPBelong(bean.userIp, ipRulesBroadcast)._4, IpTools.getIPBelong(bean.backSourceIP, ipRulesBroadcast)._5, IpTools.getIPBelong(bean.backSourceIP, ipRulesBroadcast)._6, IpTools.getIPBelong(bean.backSourceIP, ipRulesBroadcast)._7, bean.httpMethod, bean.httpVersion, bean.host, bean.mainDomain, bean.uri, bean.urlType, bean.userAgent, bean.TerminalType, bean.referer, bean.contentType, bean.statusCode, bean.codeSummary, bean.cacheState, bean.cacheCodeSummary, bean.cacheHost, bean.cacheFlow, bean.requestStartDate, bean.requestEndDate, bean.CacheTimeValue, bean.firstResponseTime, bean.URLHashCode, bean.cacheType, bean.backSourceIP, bean.backTargetIP, bean.backHttpMethod, bean.backHttpVersion, bean.backHost, bean.backUri, bean.backContentType, bean.backState, bean.backCodeSummary, bean.backCloseState, bean.backCacheControl, bean.backMaxGge, bean.backFileSize, bean.backSaveState, bean.backServerPort, bean.backFlow, bean.backRequestStartDate, bean.backRequestEndDate, bean.backTimeValue, bean.backFirstResponseTime, 1L)).toDF()
      val logDataFrame = rdd.toDF()
      logDataFrame.createOrReplaceTempView("logBean")
      // Do word count on DataFrame using SQL and print it
      //      fileType,date,deviceIp,userIp,serverIp,httpMethod,httpVersion,host,userAgent,statusCode,cacheState,cacheFlow,CacheTimeValue,firstResponseTime,cacheType,backSourceIP,backTargetIP,backHttpMethod,backHttpVersion,backHost,backStateCode,backCloseState,backFlow,backTimeValue,backFirstResponseTime,requestNum:Long,
      val wordCountsDataFrame = spark.sql("select fileType,date,deviceIp,userIp,serverIp,httpMethod,httpVersion,host,userAgent,statusCode,cacheState,cacheType,backSourceIP,backTargetIP,backHttpMethod,backHttpVersion,backHost,backStateCode,backCloseState,sum(cacheFlow) as cacheFlow,avg(cacheFlow/CacheTimeValue) as DownLoadRate,avg(firstResponseTime) as firstResponseTime,sum(backFlow) as backFlow,avg(backFlow/backTimeValue) as backDownLoadRate,avg(backFirstResponseTime) as backFirstResponseTime,sum(requestNum) as requestNum from logBean group by fileType,date,deviceIp,userIp,serverIp,httpMethod,httpVersion,host,userAgent,statusCode,cacheState,cacheType,backSourceIP,backTargetIP,backHttpMethod,backHttpVersion,backHost,backStateCode,backCloseState")

      //      val wordCountsDataFrame = spark.sql("select fileType,date,deviceIp,city,operator,system,userType,httpMethod,httpVersion,host,TerminalType,statusCode,codeSummary,cacheState,cacheStateSummary,cacheType,URLHashCode,backHttpMethod,backHttpVersion,backHost,backState,backStateSummary,backCloseState,sum(cacheFlow) as cacheFlow,avg(cacheFlow/CacheTimeValue) as DownLoadRate,avg(firstResponseTime) as firstResponseTime,sum(backFlow) as backFlow,avg(backFlow/backTimeValue) as backDownLoadRate,avg(backFirstResponseTime) as backFirstResponseTime,sum(requestNum) as requestNum from logBean group by date,fileType,deviceIp,city,operator,system,userType,httpMethod,httpVersion,host,TerminalType,statusCode,codeSummary,cacheState,cacheStateSummary,cacheType,URLHashCode,backHttpMethod,backHttpVersion,backHost,backState,backStateSummary,backCloseState")
      //      wordCountsDataFrame.repartition(1)
      println(s"3333333==" + new Date(System.currentTimeMillis()) + "=========" + nowDate)
      //      wordCountsDataFrame.saveToEs("spark-" + nowDate + "/cacheFiveData")
      wordCountsDataFrame.show()
      println(s"444444==" + new Date(System.currentTimeMillis()) + "=========" + nowDate)
      //      rdd.persist()
      //      rdd.unpersist(true)

    }

    ssc.start()
    ssc.awaitTermination()
  }
}