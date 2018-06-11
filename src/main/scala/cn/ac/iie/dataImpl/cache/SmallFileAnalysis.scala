package cn.ac.iie.dataImpl.cache

import cn.ac.iie.Service.FlumeReceiverService
import cn.ac.iie.spark.streaming.scala.kafka.Service.StreamingContextService
import cn.ac.iie.spark.streaming.util.IpTools
import cn.ac.iie.spark.streaming.util.WebSiteInfo.getWebSiteInfo
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

object SmallFileAnalysis {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val checkpointDirectory = "G://atest//Checkpoint_Data"

  var ipwebinfoArray: HashMap[String, (String, String, String, String, String, String)] = getWebSiteInfo
  val ipRulesArray = IpTools.getIPControl()

  //WEB Host匹配规则
  var ipwebBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]] = null
  //IP规则
  var ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = null

  // 加载配置文件
  val properties = CacheSourceTools.loadProperties()

  // HDFS地址
  val v_hdfs_host: String =properties.getProperty("hdfs.host")
  val v_hdfs_port: String =properties.getProperty("hdfs.port")
  val v_hive_dbname: String =properties.getProperty("hdfs.hive.dbname")
  val hive_master: String = "hdfs://" + v_hdfs_host +":" + v_hdfs_port

  def OverrideToEsByHDFS(path:String): Unit ={
    //IP规则
    val ipRulesArray = IpTools.getIPControl()
    val ssc=StreamingContextService.createHDFSContext(path)
//    var ipwebinfoArray: HashMap[String, (String, String, String, String, String, String)] = getWebSiteInfo
    var ipwebBroadcast= ssc.sparkContext.broadcast(ipwebinfoArray)
    var ipRulesBroadcast = ssc.sparkContext.broadcast(ipRulesArray)
  }

  def OverrideToEsByFlume(arg:String): Unit ={
    val conf= StreamingContextService.createSparkConf("OverviewToESByFlume_Small")
    val ssc = StreamingContextService.createFlumeReceiverContext(conf)
    val spark: SparkSession = SparkSessionSingleton.getInstance(conf)
    //广播变量
    @transient
    val ipwebinfoArray= getWebSiteInfo
    //WEB Host匹配规则
    @transient
    val ipwebBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(ipwebinfoArray)
    //IP规则
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = ssc.sparkContext.broadcast(IpTools.getIPControl())
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeReceiverService.getFlumeEvent(ssc, properties.getProperty("flume.test.small.node"), 19999)
//    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, properties.getProperty("flume.local.node2"), 19999)    //val flumeStream =   .createStream(streamingContext, [chosen machine's hostname], [chosen port])
    DataProcessingImpl.SmallDataProcessing(spark,flumeStream,ipwebinfoArray,ipwebBroadcast,ipRulesBroadcast)
    ssc.start()
    ssc.awaitTermination()
  }

  def OverrideToEsByHdfs(): Unit ={

    @transient
    val ipwebinfoArray= getWebSiteInfo

    val context = StreamingContext.getOrCreate(checkpointDirectory, createContext)
    //    val DStream = context.textFileStream("hdfs://master:9000/quality/clipper_erp/2017-07-11")
    val DStream = context.textFileStream("G://atest//ansource")
    val properties = CacheSourceTools.loadProperties()
    @transient
    val sparkConf = new SparkConf().setAppName("fromHdfsOverview").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("es.index.auto.create", "true")
    sparkConf.set("user.name", properties.getProperty("user.name"))
    //    sparkConf.setMaster("spark://10.0.30.101:7077")
    sparkConf.setMaster("local[*]")
    sparkConf.set("es.nodes", properties.getProperty("es.nodes"))
    sparkConf.set("es.port", properties.getProperty("es.port"))
    @transient
    val spark = SparkSessionSingleton.getInstance(sparkConf)

    //WEB Host匹配规则
    @transient
    val ipwebBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = spark.sparkContext.broadcast(ipwebinfoArray)
    //IP规则
    val ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = spark.sparkContext.broadcast(IpTools.getIPControl())

    import spark.implicits._
    val wordCount = DStream.map(_.split("\\|"))
      .filter(_.length >= 38)
      .filter(lines => {
        lines(34).replaceAll("'", "").toLowerCase == "null" || lines(14).replaceAll("'", "").toLowerCase == "null" || lines(34).replaceAll("'", "") == "" || lines(14).replaceAll("'", "") == "" || StringUtils.isNumeric(lines(14).replaceAll("'", "")) == true && StringUtils.isNumeric(lines(34).replaceAll("'", "")) == true
        // lines(34).toLowerCase=="null"||lines(14).toLowerCase=="null"||lines(34)==""||lines(14)==""||StringUtils.isNumeric(lines(14))  == true && StringUtils.isNumeric(lines(34))  == true &&Pattern.matches("\\d+\\.\\d+\\.\\d+\\.\\d+",lines(3))//lines(34) != "0" && lines(34) != ""&& &&lines(35) != "NULL" &&  lines(37) != "NULL"&& lines(11)!="" &&   lines(14) != "NULL" && lines(14) != "0" && lines(14) != ""
      })
      .map(info => {
        CacheSourceTools.getCacheSourceEntity(1, info(2), info(21), info(6), info(12), info(4), info(14), info(34), info(11), info(27), info(29), info(30), info(16), info(15), info(8), info(1), info(35), info(36), info(37), info(17), ipRulesBroadcast, ipwebBroadcast, info(28), info(25), "")
      })
    /*      wordCount.foreachRDD(log => {
            val joinBatch: DataFrame = CacheSourceTools.getResultDataFrame(log.toDF())
            joinBatch.show()
          })
        */
    wordCount.print()
    context.start()
    context.awaitTermination()
  }
  // 从 HDFS_SA 到 HDFS_DW
  def BatchDW_SA2DW(PT_prov: String, PT_CACHE_DATE: String): Unit ={
    val v_spk_executor_ins=properties.getProperty("spark.executor.instances")
    val v_spk_executor_cores=properties.getProperty("spark.executor.cores")
    val v_spk_cores_max=properties.getProperty("spark.cores.max")
    val v_spk_executor_memory=properties.getProperty("spark.executor.memory")

    // val spar_conf = new SparkConf().setMaster("local[*]").setAppName("BatchDWAnalysis.big_url.scala")
    val spar_conf = new SparkConf().setMaster("yarn").setAppName("BatchDW_SA2DW.small.scala")
    val spark = SparkSession.builder()
      .config("spark.executor.instances",v_spk_executor_ins)
      .config("spark.executor.cores",v_spk_executor_cores)
      .config("spark.cores.max", v_spk_cores_max)
      .config("spark.executor.memory",v_spk_executor_memory)
      .config(spar_conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val hdfs_sa_file = spark.read.orc(hive_master + "/user/hive/warehouse/"
      + v_hive_dbname +".db/tb_sa_cache_small/ds=" +PT_CACHE_DATE+"/prov="+PT_prov+"/")

    hdfs_sa_file.createOrReplaceTempView("table_sa_tmp")
    val sql_text_dw = "select factory, userip, cache_type, deviceip, city, access_type, user_terminal, file_type, " +
      "   webSiteID, webSiteName, host, main_domain, lower(cachestate), lower(statuscode), lower(backstate), " +
      "   lower(httpmethod), lower(backclosestate), lower(backcachecontrol), backmaxgge, count(*) as all_num, " +
      "   'Kb' as flowunit, sum(cacheflow)*8/1024 as all_flow, " +
      "    avg(case when cacheflow is null then null " +
      "        when requestenddate==requeststartdate then cacheflow*8/1024*1000/1 " +
      "        else cacheflow*8/1024*1000/(1000*(unix_timestamp(requestenddate, 'yyyy/MM/dd HH:mm:ss.SSS') " +
      "             - unix_timestamp(requeststartdate, 'yyyy/MM/dd HH:mm:ss.SSS'))+int(substring(requestenddate, 21))" +
      "             -int(substring(requeststartdate, 21))) end) as down_speed,  " +
      "    avg(case when firstresponsetime is null or requeststartdate is null or length(firstresponsetime) < 23 " +
      "          or length(requeststartdate) < 23 then null " +
      "        else (1000*(unix_timestamp(firstresponsetime, 'yyyy/MM/dd HH:mm:ss.SSS') " +
      "              - unix_timestamp(requeststartdate, 'yyyy/MM/dd HH:mm:ss.SSS'))+int(substring(firstresponsetime, 21))" +
      "              -int(substring(requeststartdate, 21))) end) as fstpack_delay,  " +
      "    avg(case when backfirstresponsetime is null or backrequeststartdate is null " +
      "        or length(backfirstresponsetime) < 23 or length(backrequeststartdate) < 23 then null " +
      "        else (1000*(unix_timestamp(backfirstresponsetime, 'yyyy/MM/dd HH:mm:ss.SSS') " +
      "            - unix_timestamp(backrequeststartdate, 'yyyy/MM/dd HH:mm:ss.SSS'))+int(substring(backfirstresponsetime, 21))" +
      "            -int(substring(backrequeststartdate, 21))) end) as back_fstpack_delay,  " +
      "    sum(backflow)*8/1024 as back_flow,  " +
      "    avg(case when backflow is null or backrequestenddate is null or backrequeststartdate is null " +
      "             or length(backrequestenddate) < 23 or length(backrequeststartdate) < 23 then null " +
      "        when backrequestenddate==backrequeststartdate then backflow*8/1024*1000/1 " +
      "        else backflow*8/1024*1000/(1000*(unix_timestamp(backrequestenddate, 'yyyy/MM/dd HH:mm:ss.SSS') " +
      "             - unix_timestamp(backrequeststartdate, 'yyyy/MM/dd HH:mm:ss.SSS'))+int(substring(backrequestenddate, 21))" +
      "             -int(substring(backrequeststartdate, 21))) end) as back_downspeed,  " +
           PT_prov + " as province, current_timestamp() as update_datetime, " + PT_CACHE_DATE + " as business_time, " +
      "  from_unixtime(unix_timestamp(requestenddate, 'yyyy/MM/dd HH:mm:ss.SSS'), 'HH') as business_hour " +
      "from table_sa_tmp " +
      "where unix_timestamp(date, 'yyyy/MM/dd HH:mm:ss') is not null " +
      "and deviceip rlike '^([1-9]|[1-9]\\\\d|1\\\\d\\\\d|2[0-1]\\\\d|22[0-3])(\\\\.(\\\\d|[1-9]\\\\d|1\\\\d\\\\d|2[0-4]\\\\d|25[0-5])){3}\\$' " +
      "and userip rlike '^([1-9]|[1-9]\\\\d|1\\\\d\\\\d|2[0-1]\\\\d|22[0-3])(\\\\.(\\\\d|[1-9]\\\\d|1\\\\d\\\\d|2[0-4]\\\\d|25[0-5])){3}\\$' " +
      "and serverip rlike '^([1-9]|[1-9]\\\\d|1\\\\d\\\\d|2[0-1]\\\\d|22[0-3])(\\\\.(\\\\d|[1-9]\\\\d|1\\\\d\\\\d|2[0-4]\\\\d|25[0-5])){3}\\$' " +
      "and unix_timestamp(requeststartdate, 'yyyy/MM/dd HH:mm:ss.SSS') is not null " +
      "and unix_timestamp(requestenddate, 'yyyy/MM/dd HH:mm:ss.SSS') is not null " +
      "and unix_timestamp(firstresponsetime, 'yyyy/MM/dd HH:mm:ss.SSS') is not null " +
      "group by factory, userip, cache_type, deviceip, city, access_type, user_terminal, file_type, " +
      "webSiteID, webSiteName, host, main_domain, lower(cachestate), lower(statuscode), lower(backstate), " +
      "lower(httpmethod), lower(backclosestate), lower(backcachecontrol), backmaxgge, " +
      "from_unixtime(unix_timestamp(requestenddate, 'yyyy/MM/dd HH:mm:ss.SSS'), 'HH')"
    val resultsDF: Unit = spark.sql(sql_text_dw).createOrReplaceTempView("table_temp_res")

    spark.sql("INSERT OVERWRITE TABLE "
      + v_hive_dbname + ".TB_DW_Cache_small PARTITION (ds=" + PT_CACHE_DATE + ", prov='" + PT_prov + "')" +
      " select * from table_temp_res")
  }

  def createContext(): StreamingContext ={

    val conf = new SparkConf()
      .setAppName("HDFSInputData")
      //      .setMaster("spark://master:7077")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(20))
    ssc.checkpoint(checkpointDirectory)
    ipwebBroadcast = ssc.sparkContext.broadcast(ipwebinfoArray)
    ipRulesBroadcast = ssc.sparkContext.broadcast(ipRulesArray)
    ssc
  }

}
