package cn.ac.iie.dataImpl.dns

import java.time.Clock

import cn.ac.iie.Service.Config
import cn.ac.iie.base.common.{IpControl, LogConfig}
import cn.ac.iie.base.dns.{CheckLog, DnsLog, DnsParseLog}
import cn.ac.iie.check.impl.{LengthChecker, NotNullChecker, RegexChecker}
import cn.ac.iie.utils.dns.{DomainUtil, JdbcUtil, LogUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

import scala.util.Random

object ProcessDnsLog extends Serializable {

  private val random = new Random()

  def parseDnsLog(province_code:String)  {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sparkContext: SparkContext = new SparkContext(Config.config_spark_param)
    val ipControlBroadcast = sparkContext.broadcast(JdbcUtil.getIpControl)
    val logTypeBroadcast = sparkContext.broadcast(Config.config_log_type)
    val totalCount = sparkContext.longAccumulator("totalCount")
    val dataLimitCount = sparkContext.longAccumulator("dataLimitCount")
    val lengthCheckerCount = sparkContext.longAccumulator("lengthCheckerCount")
    val notNullCheckerCount = sparkContext.longAccumulator("notNullCheckerCount")
    val regexCheckerCount = sparkContext.longAccumulator("regexCheckerCount")
    val streaming = new StreamingContext(sparkContext,Seconds(5))
    //hdfs://10.0.30.101:8020/temp/ljy_test/log
    val dStream = streaming.textFileStream("hdfs://10.10.10.6:8020/user/hadoop-user/coll_data/dns_log/jx/791/20180411")
    val res = dStream.filter(line=>ProcessDnsLog.check(totalCount,dataLimitCount,lengthCheckerCount,notNullCheckerCount,regexCheckerCount,line,logTypeBroadcast)).map(line=>{
      val dns_log = ProcessDnsLog.parseLine(line,logTypeBroadcast)
      val dnsParseLog = ProcessDnsLog.transform(dns_log,ipControlBroadcast,"330000")
      dnsParseLog
    })
    res.foreachRDD(rdd=>{
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      rdd.toDF().createOrReplaceTempView("dns_log")
      val res = spark.sql("""
        select
          biz_time as pp_0101,
          domain as dd_0001,
          fan_domain as dd_0005,
          level_domain as dd_0004,
          target_ip as ip_0001,
          target_province_code as op_0201,
          oper_code as op_0001,
          sys_code as op_0101,
          user_type as uu_type,
          count(1) as rr_x112,
          province_code as op_0201x
        from
          dns_log
        group by
          biz_time,
          domain,
          fan_domain,
          level_domain,
          target_ip,
          oper_code,
          target_province_code,
          sys_code,
          user_type,
          province_code """)

      //EsSparkSQL.saveToEs(res,"customer_ljy3/data")
      //res.writeData(logTypeBroadcast.value)
      val millis = Clock.systemUTC().millis()
      //hdfs://10.0.30.101:8020/temp/ljy_test/out/"+millis
      res.write.parquet("hdfs://10.10.10.6:8020/user/hadoop-user/dns/out")
    })

    streaming.start()
    streaming.awaitTermination()
  }

  def formatData2(line :String,log_type:Broadcast[LogConfig]):Array[String]={
    val sp = line.split(log_type.value.data_separator)
    formatData(sp)
  }

  def formatData(sp:Array[String]):Array[String]={
    if(sp(1).endsWith(".")||sp.endsWith("?"))
      sp(1) = sp(1).substring(0,sp(1).length-1)
    sp(3) = sp(3).split(";")(0)
    for((value,index)<-sp.zipWithIndex){
      sp(index) = value.trim
    }
    sp
  }


  def check(totalCount: LongAccumulator, dataLimitCount: LongAccumulator, lengthCheckerCount: LongAccumulator, notNullCheckerCount: LongAccumulator, regexCheckerCount: LongAccumulator,line: String,log_type:Broadcast[LogConfig]): Boolean = {
    totalCount.add(1)
    val logConfig = log_type.value
    val data_separator = logConfig.data_separator
    val sp = line.split(data_separator,-1)
    if(sp.length!=logConfig.data_limit){
      dataLimitCount.add(1)
      //LogUtil.log(s"data_limit is not match ,data_source:$line data_separator:$data_separator current_limit:${sp.length} data_limit:${logConfig.data_limit} ")
      return false
    }else{
      val format = formatData(sp)
      val checkers = logConfig.checkers
      for (checker_Info <- checkers){
        val result = checker_Info.checker match {
          case checker:LengthChecker => checker.check(format,checker_Info.check_index,checker_Info.checker_param,lengthCheckerCount)
          case checker:NotNullChecker => checker.check(format,checker_Info.check_index,checker_Info.checker_param,notNullCheckerCount)
          case checker:RegexChecker => checker.check(format,checker_Info.check_index,checker_Info.checker_param,regexCheckerCount)
        }
        if(!result)
          return false
      }
    }
    true
  }

  def parseLine(line:String,log_type:Broadcast[LogConfig]): DnsLog={
    DnsLog(formatData2(line,log_type))
  }

  def transform(dns_log: DnsLog,ipControl: Broadcast[Array[IpControl]],province_code:String): DnsParseLog = {
    val (fan_doamin,level) = DomainUtil.getFanDomain(dns_log.domain)
    val target_ip_belong = JdbcUtil.findIpControlByIp(dns_log.target_ip,ipControl.value)
    val source_ip_belong = JdbcUtil.findIpControlByIp(dns_log.source_ip,ipControl.value)
    DnsParseLog(
      dns_log.time.substring(0,8),
      dns_log.domain,
      fan_doamin,
      level.toString,
      dns_log.target_ip,
      target_ip_belong.operator ,
      target_ip_belong.location,
      target_ip_belong.system,
      source_ip_belong.user_type,
      province_code,
      dns_log.return_code
    )
  }


  def updateStatus(checkLog:CheckLog,schedule_state:String,id:Int): Unit ={
    JdbcUtil.insertDataCheckLog(checkLog)
    JdbcUtil.updateloadDataWfLog(schedule_state,131)
  }

}
