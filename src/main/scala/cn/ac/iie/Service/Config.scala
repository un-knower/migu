package cn.ac.iie.Service

import cn.ac.iie.base.common.{CheckerInfo, JdbcConfig, LogConfig}
import cn.ac.iie.check.Checker
import cn.ac.iie.utils.dns.ConfigTools
import org.apache.spark.SparkConf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

object Config {

  val config_log_type = loadConfigLogType()
  val config_jdbc_param = loadConfigJdbcParam()
  val config_spark_param = loadConfigSparkParam()
  val config_profile_param = loadConfigProfileParam()

  private def loadConfigProfileParam() = {
    val prop = ConfigTools.loadProperties("config_profile_param.properties")
    val keys = prop.stringPropertyNames()
    val it = keys.iterator()
    val map = mutable.Map[String,String]()
    while (it.hasNext){
      val ele = it.next()
      map+=(ele->prop.getProperty(ele))
    }
    println(map)
    map.toMap
  }

  private def loadConfigSparkParam() = {
    val conf = new SparkConf()
    val prop = ConfigTools.loadProperties("config_spark_param.properties")
    val keys = prop.stringPropertyNames()
    val it = keys.iterator()
    while (it.hasNext){
      val ele = it.next()
      println(s"$ele->${prop.getProperty(ele)}")
      conf.set(ele,prop.getProperty(ele))
    }
    conf
  }

  private def loadConfigJdbcParam() = {
    val prop = ConfigTools.loadProperties("config_jdbc_param.properties")
    JdbcConfig(prop.getProperty("jdbc.driver"),prop.getProperty("jdbc.ipcontroll.url"),prop.getProperty("jdbc.username"),prop.getProperty("jdbc.password"),prop.getProperty("jdbc.jxdns.url")
    ,prop.getProperty("jdbc.apollo.username"),prop.getProperty("jdbc.apollo.password"),prop.getProperty("jdbc.apollo.url"))
  }

  private def loadConfigLogType() ={
    val prop = ConfigTools.loadProperties("config_log_type.properties")
    val enable_config =  prop.getProperty("enable_config")
    val dns_config = prop.getProperty(enable_config)
    println(dns_config)
    val option = JSON.parseFull(dns_config)
    val map = option.get.asInstanceOf[Map[String,Any]]
    val checkers = map("checkers").asInstanceOf[List[Map[String,Any]]]
    val list = new ListBuffer[CheckerInfo]
    for(check<-checkers){
      val index = check("check_index").asInstanceOf[List[Double]].map(_.toInt)
      val ci = CheckerInfo(Class.forName(check("class_name").asInstanceOf[String]).newInstance().asInstanceOf[Checker],check("checker_param").asInstanceOf[Map[String,Any]],index)
      list.append(ci)
    }
    val checkInfo = list.toList
    val conf = LogConfig(map("data_limit").asInstanceOf[Double].toInt,map("data_separator").asInstanceOf[String],checkInfo)

    println(conf)
    conf
  }

}
