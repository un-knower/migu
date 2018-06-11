package cn.ac.iie.spark.streaming.scala.kafka.Service

import java.sql.{Connection, DriverManager}

import cn.ac.iie.spark.streaming.scala.kafka.Interface.WebSiteInfoInterface
import cn.ac.iie.spark.streaming.util.CacheSourceTools
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.HashMap

object WebSiteInfo extends WebSiteInfoInterface{
  override def getWebSiteInfo(): HashMap[String, (String, String, String, String, String, String)] = {
    var conn: Connection = null
    val re = HashMap[String, (String, String, String, String, String, String)]()
    println("getWebSiteInfo")
    val properties = CacheSourceTools.loadProperties()
    try {
      //      conn = DriverManager.getConnection("jdbc:mysql://10.0.30.14:3306/rmp?useUnicode=true&characterEncoding=utf-8", "root", "qwertyu")
      conn = DriverManager.getConnection(properties.getProperty("mysqlConn"), properties.getProperty("mysql.user"),properties.getProperty("mysql.pwd"))
      val statement = conn.createStatement()
      val result = statement.executeQuery("""
       SELECT
	      a.WEBSITE_ID,
	      b.WEBSITE_NAME,
	      b.WEBSITE_TYPE,
	      c.WS_0005,
	      b.WEBSITE_URL,
	      a.DOMAIN_ID
      FROM  rm_website_manager b
      LEFT OUTER JOIN rm_website_domain_relation a ON b.WEBSITE_ID = a.WEBSITE_ID
      LEFT OUTER JOIN rmp.tb_ws_w_0003 c ON b.WEBSITE_TYPE = c.WS_0004
      where a.DOMAIN_ID is not NULL""")
      //      LEFT OUTER JOIN rmp_dm.tb_ws_w_0003 c ON b.WEBSITE_TYPE = c.WS_0004
      while (result.next()) {
        re.put(result.getString("domain_id"), (result.getString("website_id"), result.getString("website_name"), result.getString("website_type"), result.getString("ws_0005"), result.getString("website_url"), result.getString("domain_id")))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null)
        conn.close()
    }
    re
  }

  override def getWebSite(main_domain: String, webSiteBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]]): (String, String, String, String, String, String) = {
    var index: Int = -1
    var info: (String, String, String, String, String, String) = ("-1", "-1", "-1", "-1", "-1", "-1")
    if (main_domain != "-1") {
      val webInfo = webSiteBroadcast.value
      if (!webInfo.get(main_domain).isEmpty) {
        info = webInfo.get(main_domain).get
      }
    }
    info
  }

  override def getWebChanelInfo():  HashMap[String, (String, String, String, String, String, String)]  ={
    var conn: Connection = null
    val re = HashMap[String, (String, String, String, String, String, String)]()
    println("connect to mysql :BigFile 233333 XD")
    try {
      //      conn = DriverManager.getConnection("jdbc:mysql://10.0.30.14:3306/rmp?useUnicode=true&characterEncoding=utf-8", "root", "qwertyu")
      val properties = CacheSourceTools.loadProperties()
      conn = DriverManager.getConnection(properties.getProperty("mysqlConn"), properties.getProperty("mysql.user"),properties.getProperty("mysql.pwd"))
      val statement = conn.createStatement()
      val result = statement.executeQuery("""
      SELECT website_name,channel_id,channel_name,`name`,perfix,extname from rm_bigfile_website_manager WHERE `status`=1""")
      while (result.next()) {
        re.put(result.getString("website_name"), (result.getString("website_name"), result.getString("channel_id"), result.getString("channel_name"),
          result.getString("name"), result.getString("perfix"), result.getString("extname")))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null)
        conn.close()
    }
    re
  }

  override def getWebChanel(main_domain: String,url:String, webSiteBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]]): (String, String, String, String, String, String) ={
    //var info: (String, String, String, String, String, String) = ("-1", "-1", "-1", "-1", "-1", "-1")
    var result=("-1", "-1", "-1", "-1", "-1", "-1")
    //info:website_name,channel_id,channel_name,name,perfix,extname
    if (main_domain != "-1") {
      val webInfo = webSiteBroadcast.value
      if (!webInfo.get(main_domain).isEmpty) {
        var info = webInfo.get(main_domain).get
        if(info._4.contains(url)&&info._5.contains(url)&&info._6.contains(url)){
          result=info
        }
      }
    }
    result
  }
}
