package cn.ac.iie.dataImpl.dns

import java.time.Clock

import cn.ac.iie.Service.Config
import cn.ac.iie.base.dns.{CheckLog, DnsLog, DnsParseLog, DnsParseRatio}
import cn.ac.iie.utils.dns.{DateUtil, JdbcUtil, LogUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object DnsOffline {

  def parseDnsLog(province_code:String,etlDate:String): Unit = {
    val start_time = DateUtil.getCurrentDateTime()
    val checkLog = CheckLog(null,0,start_time,null,province_code,null,etlDate)
    var schedule_state:String = null
    try{
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      //Config.config_spark_param.registerKryoClasses(Array(classOf[IpControl],classOf[DnsLog],classOf[DnsParseLog],classOf[Checker]))
      val sparkContext: SparkContext = new SparkContext(Config.config_spark_param)
      val ipControlBroadcast = sparkContext.broadcast(JdbcUtil.getIpControl)
      val logTypeBroadcast = sparkContext.broadcast(Config.config_log_type)
      val config_profile_param = Config.config_profile_param
      val hdfs_url = config_profile_param("hdfs.url")
      val hdfs_dnslog_path = config_profile_param("hdfs.dnslog.path")
      val hive_table_name = config_profile_param("hive.table.name")
      val totalCount = sparkContext.longAccumulator("totalCount")
      val dataLimitCount = sparkContext.longAccumulator("dataLimitCount")
      val lengthCheckerCount = sparkContext.longAccumulator("lengthCheckerCount")
      val notNullCheckerCount = sparkContext.longAccumulator("notNullCheckerCount")
      val regexCheckerCount = sparkContext.longAccumulator("regexCheckerCount")
      val returnCodeAccumulator = new ReturnCodeAccumulator
      sparkContext.register(returnCodeAccumulator)
      val spark = SparkSession.builder().config(sparkContext.getConf).enableHiveSupport().getOrCreate()
      import spark.implicits._
      implicit val encoder = org.apache.spark.sql.Encoders.kryo[DnsLog]
      val input = hdfs_url+hdfs_dnslog_path+etlDate
      //val input = "hdfs://10.0.30.101:8020/temp/ljy_test/log"//791_12_20171025230217a
      //val input = "file:///C:\\Users\\Administrator\\Desktop\\791_12_20171025230217a"
      val dataSet = spark.read.textFile(input)
      val res = dataSet.filter(line=>ProcessDnsLog.check(totalCount,dataLimitCount,lengthCheckerCount,notNullCheckerCount,regexCheckerCount,line,logTypeBroadcast)).map(line=>{
        val dns_log = ProcessDnsLog.parseLine(line,logTypeBroadcast)
        val dnsParseLog = ProcessDnsLog.transform(dns_log,ipControlBroadcast,province_code)
        returnCodeAccumulator.add(dnsParseLog.return_code)
        dnsParseLog
      })
      res.createOrReplaceTempView("dns_log")
      val res2 = spark.sql("""
        select
          biz_time as pp_0101,
          domain as dd_0001,
          fan_domain as dd_0005,
          level_domain as dd_0004,
          target_ip as ip_0001,
          oper_code as op_0001,
          target_province_code as op_0201,
          sys_code as op_0101,
          user_type as uu_type,
          province_code as op_0201x,
          count(1) as rr_x112
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
          province_code""")
      import spark.sql
      res2.createOrReplaceTempView("dns_result")
      sql("INSERT OVERWRITE TABLE "+hive_table_name+" PARTITION (ds='"+etlDate+"',prov='"+province_code+"') select * from dns_result")
      val message = s"总计处理${totalCount.value}条数据,数据分割长度错误${dataLimitCount.value}条,非空校验器验证错误${notNullCheckerCount.value}条,正则校验器验证错误${regexCheckerCount.value}条,长度校验器验证错误${lengthCheckerCount.value}条"
      LogUtil.log(message)
      checkLog.message = message
      checkLog.correct_ratio = 1-((BigDecimal(dataLimitCount.value)+BigDecimal(notNullCheckerCount.value)+BigDecimal(regexCheckerCount.value))/BigDecimal(totalCount.value)).toDouble
      LogUtil.log(s"parse_code_ratio ${returnCodeAccumulator.value}")
      if(!returnCodeAccumulator.isZero)
        JdbcUtil.insert_parse_code_ratio(returnCodeAccumulator.value)
      checkLog.state = "sechdule_succeeded"
      schedule_state = "SUCCEEDED"
    }catch {
      case ex:Exception => ex.printStackTrace()
        checkLog.state = "schedule_failed"
        schedule_state = "FAILED"
        checkLog.message = ex.getMessage
    }finally {
      val end_time = DateUtil.getCurrentDateTime()
      checkLog.endTime = end_time
      ProcessDnsLog.updateStatus(checkLog,schedule_state,131)
    }

  }

}

/*if(province_code=="330000"){//对于江西省，额外统计return_code信息，添加到前端数据库
        res.persist()
        val update_date = DateUtil.getCurrentDateTime()
        val return_code = spark.sql(" " +
          "select " +
          "return_code as rate_type," +
          "count(1) as parse_number," +
          ""+etlDate.toInt+" as etldate," +
          "'"+update_date+"' as update_date " +
          "from " +
          "dns_log " +
          "group by " +
          "return_code")
        val dnsParseRatio = return_code.map(row=>{
          var return_code = row.getString(0)
          return_code = return_code.split("_")(0)
          DnsParseRatio(return_code,row.getLong(1),row.getInt(2),row.getString(3))
        })
        dnsParseRatio.createOrReplaceTempView("dnsParseRatio")
        val dnsParseRatioResult = spark.sql("""
            select
              rate_type,
              sum(parse_number) as parse_number,
              etldate,
              update_date
            from
              dnsParseRatio
            group by
              rate_type,
              etldate,
              update_date
          """)
        val jdbc = Config.config_jdbc_param
        dnsParseRatioResult.write.mode(SaveMode.Append).format("jdbc")
          .option("url", jdbc.jx_dns_url)
          .option("dbtable", "ifp_dns_jx.dm_parse_code_ratio")
          .option("user", jdbc.username)
          .option("password", jdbc.password)
          .save()
      }*/
