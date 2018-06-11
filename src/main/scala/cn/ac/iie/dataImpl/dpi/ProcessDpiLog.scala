package cn.ac.iie.dataImpl.dpi

import java.time.Instant

import cn.ac.iie.Service.Config
import cn.ac.iie.base.common.{IpControl, LogConfig}
import cn.ac.iie.base.dpi.{DpiLog, DpiParseLog, DpiUrlLog}
import cn.ac.iie.check.impl.{DpiNotNullChecker, NotNullChecker}
import cn.ac.iie.utils.dns._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

object ProcessDpiLog {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(Config.config_spark_param).enableHiveSupport().getOrCreate()
    val sparkContext = spark.sparkContext
    val logTypeBroadcast = sparkContext.broadcast(Config.config_log_type)
    val ipControlBroadcast = sparkContext.broadcast(JdbcUtil.getIpControl)
    val notNullCheckerCount = sparkContext.longAccumulator
    import spark.implicits._
    val dpiLog = spark.read.csv("file:///C:\\Users\\Administrator\\Desktop\\103_20170917181954_0159.csv")
    //implicit val encoder = org.apache.spark.sql.Encoders.kryo[DpiLog]
    implicit val encoder = org.apache.spark.sql.Encoders.product[DpiLog]
    val res = dpiLog.filter(row=>ProcessDpiLog.check(row.getString(0),logTypeBroadcast,null,null,null,notNullCheckerCount,null))
      .map(row =>{
        val dpiLog = ProcessDpiLog.parseLine(row.getString(0),logTypeBroadcast)
        ProcessDpiLog.transform(dpiLog,ipControlBroadcast)
      })
    res.createOrReplaceTempView("dpi")
    //spark.sql("select * from dpi").show(false)
    res.persist()
    val etlDate = DateUtil.getEtlDate()
    processDomain(spark)
    processUrl(spark)
  }

  def processUrl(spark: SparkSession):Unit = {
    import spark.implicits._
    val etlDate = DateUtil.getEtlDate()
    val dw_url = spark.sql(s"""
        select
          host,
          uri,
          _interface,
          appTypeCode,
          visitDate,
          startTime,
          endTime,
          protoType,
          userType,
          userProv,
          userOperator,
          userSystem,
          operator,
          province,
          system,
          httpVersion,
          userAgent,
          urlType,
          contentType,
          refer,
          cookie,
          browserType,
          sum(ulBytes) as ulBytes,
          sum(dlBytes) as dlBytes,
          avg(contentLen) as contentLen
        from
          dpi
        group by
          host,
          uri,
          _interface,
          appTypeCode,
          visitDate,
          startTime,
          endTime,
          protoType,
          userType,
          userProv,
          userOperator,
          userSystem,
          operator,
          province,
          system,
          httpVersion,
          userAgent,
          urlType,
          contentType,
          refer,
          cookie,
          browserType
      """)
    val dw_arr = dw_url.map(row=>{
      val startTime = row.getAs[String]("startTime")
      val endTime = row.getAs[String]("endTime")
      val ulBytes = row.getAs[Double]("ulBytes")
      val dlBytes = row.getAs[Double]("dlBytes")
      val array = DateUtil.genDpiSeqTimeSource(startTime,endTime)
      val avgdlBytes = (BigDecimal(dlBytes)/BigDecimal(array.length)).toString()
      val res = ArrayBuffer[DpiUrlLog]()
      for(i<-array.indices){
        if(i==0){
          res.append(DpiUrlLog(
            row.getAs[String]("host"),
            row.getAs[String]("uri"),
            row.getAs[String]("_interface"),
            row.getAs[String]("appTypeCode"),
            row.getAs[String]("visitDate"),
            array(i),
            row.getAs[String]("protoType"),
            row.getAs[String]("userType"),
            row.getAs[String]("operator"),
            row.getAs[String]("province"),
            row.getAs[String]("system"),
            ulBytes+"",
            avgdlBytes,
            row.getAs[String]("httpVersion"),
            row.getAs[String]("userAgent"),
            row.getAs[String]("urlType"),
            row.getAs[String]("contentType"),
            row.getAs[String]("refer"),
            row.getAs[String]("cookie"),
            row.getAs[Double]("contentLen")+"",
            row.getAs[String]("browserType"),
            "1",
            etlDate,
            row.getAs[String]("userProv"),
            row.getAs[String]("userOperator"),
            row.getAs[String]("userSystem")
          ))
        }else{
          res.append(DpiUrlLog(
            row.getAs[String]("host"),
            row.getAs[String]("uri"),
            row.getAs[String]("_interface"),
            row.getAs[String]("appTypeCode"),
            row.getAs[String]("visitDate"),
            array(i),
            row.getAs[String]("protoType"),
            row.getAs[String]("userType"),
            row.getAs[String]("operator"),
            row.getAs[String]("province"),
            row.getAs[String]("system"),
            "0",
            avgdlBytes,
            row.getAs[String]("httpVersion"),
            row.getAs[String]("userAgent"),
            row.getAs[String]("urlType"),
            row.getAs[String]("contentType"),
            row.getAs[String]("refer"),
            row.getAs[String]("cookie"),
            row.getAs[Double]("contentLen")+"",
            row.getAs[String]("browserType"),
            "0",
            etlDate,
            row.getAs[String]("userProv"),
            row.getAs[String]("userOperator"),
            row.getAs[String]("userSystem")
          ))
        }
      }
      res.toArray
    })
    val flat  = dw_arr.flatMap(arr=>arr)
    println(flat.printSchema())
    flat.createTempView("url_res")
    spark.sql(s"""
         select
            host,
            uri,
            _interface,
            appTypeCode,
            visitDate,
            visitTime,
            protoType,
            userType,
            operator,
            province,
            system,
            sum(ulBytes) as ulBytes,
            sum(dlBytes) as dlBytes,
            httpVersion,
            userAgent,
            urlType,
            contentType,
            refer,
            cookie,
            avg(contentLen) as contentLen,
            browserType,
            sum(requestNum) as requestNum,
            '$etlDate' as etlDate,
            userProv,
            userOperator,
            userSystem
         from
           url_res
         group by
           host,
           uri,
           _interface,
           appTypeCode,
           visitDate,
           visitTime,
           protoType,
           userType,
           operator,
           province,
           system,
           httpVersion,
           userAgent,
           urlType,
           contentType,
           refer,
           cookie,
           browserType,
           userProv,
           userOperator,
           userSystem
       """).createOrReplaceTempView("url_res2")
    spark.sql(s"INSERT OVERWRITE TABLE remaworks_2_0_hn.tb_dw_url_dpi PARTITION (ds='$etlDate') select * from url_res2")
  }

  def processDomain(spark:SparkSession): Unit = {
    val etlDate = DateUtil.getEtlDate()
    val dw_domain = spark.sql(
     s"""
       select
         _interface,
         visitDate,
         protoType,
         usrIpv4,
         userType,
         srvIpv4,
         srvPort,
         operator,
         province,
         system,
         sum(ulBytes) as ulBytes,
         sum(dlBytes) as dlBytes,
         avg(serverDelay) as serverDelay,
         avg(clientDelay) as clientDelay,
         avg(stACK21stREQ) as stACK21stREQ,
         avg(stREQ21stACK) as stREQ21stACK,
         httpVersion,
         httpRespCode,
         avg(get2OKDelay) as get2OKDelay,
         host,
         userAgent,
         '$etlDate' as etl_date
       from
         dpi
       group by
         _interface,
         visitDate,
         protoType,
         usrIpv4,
         userType,
         srvIpv4,
         srvPort,
         operator,
         province,
         system,
         httpVersion,
         httpRespCode,
         host,
         userAgent
     """).createOrReplaceTempView("domain_res")
    spark.sql(s"INSERT OVERWRITE TABLE remaworks_2_0_hn.tb_dw_domain_dpi PARTITION (ds='$etlDate') select * from domain_res")
  }

  def transform(dpiLog: DpiLog, ipControlBroadcast: Broadcast[Array[IpControl]]) = {
    val ipControl = ipControlBroadcast.value
    val startInstant = Instant.ofEpochMilli(dpiLog.procedure_start_time.substring(0,13).toLong)
    val endInstant = Instant.ofEpochMilli(dpiLog.procedure_end_time.substring(0,13).toLong)
    val visitDate = DateUtil.getCustomDate("yyyy-MM-dd")(startInstant)
    val dateFun = DateUtil.getCustomDate("HHmm")_
    val startTime = dateFun(startInstant)
    val endTime = dateFun(endInstant)
   /* val startTime = dpiLog.procedure_start_time
    val endTime = dpiLog.procedure_end_time*/
    val serverIpControl = JdbcUtil.findIpControlByIp(dpiLog.app_server_IP_IPv4,ipControl)
    val userIpControl = JdbcUtil.findIpControlByIp(dpiLog.user_IPv4,ipControl)
    val user_type = userIpControl.user_type
    val operator = serverIpControl.operator
    val province = serverIpControl.location
    val system = serverIpControl.system
    val userProv = userIpControl.location
    val userOperator = userIpControl.operator
    val userSystem = userIpControl.system
    val userAgent = DomainUtil.getUserAgent(dpiLog.user_agent)
    val urlType = DomainUtil.getUriType(dpiLog.uri)
    DpiParseLog(dpiLog.host,dpiLog.uri.trim,dpiLog._interface,visitDate,startTime,endTime,dpiLog.app_type_code,dpiLog.protocol_type,
      dpiLog.user_IPv4,dpiLog.app_server_IP_IPv4,dpiLog.app_server_port,user_type,operator,province,system,dpiLog.ul_data,dpiLog.dl_data,dpiLog.tcp_response_time,
      dpiLog.tcp_ack_time,dpiLog.first_req_time,dpiLog.first_response_time,dpiLog.first_http_response_time,dpiLog.http_version,dpiLog.message_status,userAgent,
      urlType,dpiLog.http_content_type,dpiLog.refer_URI,dpiLog.cookie,dpiLog.content_length,dpiLog.IE,DateUtil.getEtlDate(),userProv,userOperator,userSystem
    )
  }

  def parseLine(line:String,log_type:Broadcast[LogConfig]): DpiLog={
    DpiLog(formatData2(line,log_type))
  }

  def formatData(arr:Array[String]) ={
    if(arr(70).endsWith(".")||arr(70).endsWith("?"))
      arr(70) = arr(70).substring(0,arr(70).length-1)
    arr
  }

  def formatData2(line:String,log_type:Broadcast[LogConfig]) ={
    val sp = line.split(log_type.value.data_separator)
    formatData(sp)
  }

  def isValidDomainForDpi(domain:String): Boolean ={
    if (domain.length() <= 256) {
      if (DomainUtil.isValidDomain(domain)||IpUtil.isValidIp(domain))
        return true
    }
    //LogUtil.log(s"$domain is not ValidDomain")
    false
  }

  def check(line:String,log_type:Broadcast[LogConfig],totalCount: LongAccumulator, dataLimitCount: LongAccumulator, lengthCheckerCount: LongAccumulator, notNullCheckerCount: LongAccumulator, regexCheckerCount: LongAccumulator): Boolean = {
    val logConfig = log_type.value
    val checkers = logConfig.checkers
    val data_separator = logConfig.data_separator
    val sp = line.split(data_separator, -1)
    val format = formatData(sp)
    for (checker_Info <- checkers) {
      val result = checker_Info.checker match {
        case checker: DpiNotNullChecker => checker.check(format, checker_Info.check_index, checker_Info.checker_param, notNullCheckerCount)
        case checker: NotNullChecker => checker.check(format, checker_Info.check_index, checker_Info.checker_param, notNullCheckerCount)
      }
      if (!result)
        return false
      val result2 = isValidDomainForDpi(format(70))//过滤非法域名
      if (!result2)
        return false
    }

    true
  }

}
