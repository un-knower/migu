package cn.ac.iie.Main.cache

import cn.ac.iie.dataImpl.cache.SparkSessionSingleton
import cn.ac.iie.spark.streaming.scala.kafka.GetTimeUtils
import cn.ac.iie.spark.streaming.scala.kafka.Impl.SavaDataImpl
import cn.ac.iie.spark.streaming.util.WebSiteInfo.{getWebChanelInfo, getWebSiteInfo}
import cn.ac.iie.spark.streaming.util.{CacheSourceTools, IpTools}
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

object FromHDFSOverview_Big extends Serializable {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  //  val checkpointDirectory = "G://atest//Checkpoint_Data"
  val checkpointDirectory = "hdfs://10.0.30.101:8020/Checkpoint_Data_Big"
  //广播变量
  @transient
  var ipwebinfoArray: HashMap[String, (String, String, String, String, String, String)] = getWebSiteInfo

  @transient
//  var ipwebBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]] = null
  //IP规则
  val ipRulesArray = IpTools.getIPControl()
//  var ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]] = null

  def main(args: Array[String]): Unit = {
    val prop = CacheSourceTools.loadProperties()
    val conf = new SparkConf()
      .setAppName("Big_Streaming")
//      .setMaster("spark://10.0.30.101:7077")
            .setMaster("yarn")
//            .setMaster("local[3]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("es.index.auto.create", "true")
      .set("es.nodes",prop.getProperty("es.nodes"))
      .set("es.port", prop.getProperty("es.port"))
      .set("spark.cores.max","10")
      .set("spark.executor.instances","4")
      .set("spark.executor.cores","2")
      .set("spark.executor.memory","10G")
      .set("spark.driver.memory","2G")


    val context=new StreamingContext(conf, Seconds(20))
    context.checkpoint(prop.getProperty("big.checkpointDirectory"))
//    context.checkpoint(checkpointDirectory)
    //WEB Host匹配规则
    var ipwebBroadcast = context.sparkContext.broadcast(ipwebinfoArray)
    var ipRulesBroadcast = context.sparkContext.broadcast(ipRulesArray)
    @transient
    val webChanelInfoArray=getWebChanelInfo
    @transient
    val chanelInfoBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] = context.sparkContext.broadcast(webChanelInfoArray)
    println(ipRulesBroadcast==null)
    val spark = SparkSessionSingleton.getInstance(conf)
//    val DStream = context.textFileStream("hdfs://10.0.30.101:8020/user/hadoop-user/coll_data/cache_log/jiangxi/791/big/")
    val DStream = context.textFileStream(prop.getProperty("hdfs.big.streaming.path"))
    try {
      DStream.foreachRDD(lines=>{
        import spark.implicits._
        val filter = lines
          .map(str => {
            str.split("\\|")
          })
          .filter(line => line.length >= 38)
        val inputDF = filter
          .filter(lines => {
            lines(34).replaceAll("'", "").toLowerCase == "null" || lines(14).replaceAll("'", "").toLowerCase == "null" || lines(34).replaceAll("'", "") == "" || lines(14).replaceAll("'", "") == "" || StringUtils.isNumeric(lines(14).replaceAll("'", "")) == true && StringUtils.isNumeric(lines(34).replaceAll("'", "")) == true
          })
          .map(info => {
            CacheSourceTools.getCacheSourceEntity(1, info(2).replaceAll("'", ""), info(21).replaceAll("'", ""), info(6).replaceAll("'", ""), info(12).replaceAll("'", ""), info(4).replaceAll("'", ""), info(14).replaceAll("'", ""), info(34).replaceAll("'", ""), info(11).replaceAll("'", ""), info(27).replaceAll("'", ""), info(29).replaceAll("'", ""), info(30).replaceAll("'", ""), info(16).replaceAll("'", ""), info(15).replaceAll("'", ""), info(8).replaceAll("'", ""), info(1).replaceAll("'", ""), info(35).replaceAll("'", ""), info(36).replaceAll("'", ""), info(37).replaceAll("'", ""), info(17).replaceAll("'", ""), ipRulesBroadcast, ipwebBroadcast, info(28).replaceAll("'", ""), info(25).replaceAll("'", ""), "big")
          }).toDF()
        inputDF.createOrReplaceTempView("bigcaches")
        val date = CacheSourceTools.getNowDate()
        val result = spark.sql("select 'huawei' as factory,business_time_1d,business_time_1h,file_type,flowUnit,sum(all_flow) as all_flow,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time,cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain,business_time,count(1) as all_num,sum(req_ok_num) as req_ok_num,(count(1)-sum(req_ok_num))as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(req_Denied_num) as req_Denied_num,sum(req_Error_num) as req_Error_num,sum(req_Error_num+req_Denied_num)as req_Otherwise_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,sum(hit_flow) as hit_flow,sum(denied_flow) as denied_flow,sum(miss_flow) as miss_flow,(sum(all_flow)-sum(hit_flow)-sum(cacheMiss_flow)) as other_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,sum(dnsparse_fail_num) as dnsparse_fail_num,sum(backlink_fail_num) as backlink_fail_num,sum(nocache_flow) as nocache_flow,avg(back_downspeed) as back_downspeed from bigcaches group by cache_type,cache_eqp_info,city,access_type,user_terminal,webSiteID,webSiteName,domain ,flowUnit,fstpack_delay,back_fstpack_delay,back_flow,update_datetime,business_time_type,business_time,business_time_1h,business_time_1d,file_type")
        if(result.count()>0){
          EsSparkSQL.saveToEs(result, "ifp_cache_"+CacheSourceTools.getNowDate()+"_big/TB_APP_Cache_Overview_5m")
        }
        println("接受数据:"+filter.count()+";已存数据:"+result.count())
        val dataFrame = filter.map(lines=>{
          CacheSourceTools.SACacheBigInfo(lines, ipRulesBroadcast, ipwebBroadcast, chanelInfoBroadcast)
        }).toDF()
        val saCount = dataFrame.count()
        if(saCount>0){
          var utilss=new GetTimeUtils()
          var path ="hdfs://"+prop.getProperty("hdfs.host")+":8020/ifp_source/DataCenter/SA/TB_SA_Cache_big/"+date+"/610000/"+utilss.getTime.replaceAll(":","")+"/data/store/"
          SavaDataImpl.SaveDataToHDFS(dataFrame,path)
        }

      })
    }catch {
      case e: Exception => "本次写入失败:"+e.printStackTrace()
    }finally {
    }
    context.start()
    context.awaitTermination()
  }
  def createContext(): StreamingContext ={

    val conf = new SparkConf()
      .setAppName("Big_Streaming")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("es.index.auto.create", "true")
      .set("es.nodes","10.0.30.106,10.0.30.107")
      .set("es.port", "9200")
    val ssc = new StreamingContext(conf, Seconds(20))
//    ssc.checkpoint(checkpointDirectory)
    ssc
  }





}
