package cn.ac.iie.utils.dns

import java.sql.{Connection, DriverManager}

import cn.ac.iie.Service.Config
import cn.ac.iie.base.common.{IpControl, WebChannel, Website}
import cn.ac.iie.base.dns.CheckLog
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

object JdbcUtil {

  def getConnection(driver:String = Config.config_jdbc_param.driver,url:String = Config.config_jdbc_param.ipcontroll_url,user:String = Config.config_jdbc_param.username,password:String = Config.config_jdbc_param.password) ={
    println("get Connection ........")
    Class.forName(driver)
    var conn: Connection = null
    conn = DriverManager.getConnection(url, user , password)
    conn
  }

  def getIpControl: Array[IpControl] ={
    var conn:Connection = null
    val ipControls = ArrayBuffer[IpControl]()
    try{
      conn = getConnection()
      val sql = "select int_startip,int_endip,location,city,operator,system,user_type from ip_control order by int_startip,int_endip asc"
      val statement = conn.prepareStatement(sql)
      val resultSet = statement.executeQuery()
      while (resultSet.next()){
        ipControls.append(IpControl(
          resultSet.getLong("int_startip"),
          resultSet.getLong("int_endip"),
          resultSet.getString("location"),
          resultSet.getString("operator"),
          resultSet.getString("system"),
          resultSet.getString("city"),
          resultSet.getString("user_type")))
      }
    }catch {
      case ex:Exception => ex.printStackTrace()
    }finally {
      if(conn!=null)
        conn.close()
    }
    ipControls.toArray
  }


  def findIpControlByIp(ip: String,ipControl: Array[IpControl],defaultValue:String = "-1"):IpControl = {
    val ipNum = IpUtil.ip2long(ip)
    var low = 0
    var high = ipControl.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ipNum >= ipControl(middle).start_ip) && (ipNum <= ipControl(middle).end_ip)) {
        return  ipControl(middle)
      }
      if (ipNum < ipControl(middle).start_ip) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    IpControl(0,0,"999999","9999","9999",defaultValue,"9")
  }


  def getWebSite: HashMap[String, Website] = {
    var conn: Connection = null
    val websiteMap = HashMap[String,Website]()
    println("getWebSiteInfo")
    try {
      //conn = DriverManager.getConnection("jdbc:mysql://10.0.30.14:3306/rmp?useUnicode=true&characterEncoding=utf-8", "root", "qwertyu")
      conn = JdbcUtil.getConnection()
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
        LEFT OUTER JOIN ifp_dic_bj.tb_ws_w_0003 c ON b.WEBSITE_TYPE = c.WS_0004
        where a.DOMAIN_ID is not NULL""")
      //      LEFT OUTER JOIN rmp_dm.tb_ws_w_0003 c ON b.WEBSITE_TYPE = c.WS_0004
      while (result.next()) {
        websiteMap.put(result.getString("domain_id"), Website(result.getString("website_id"), result.getString("website_name"), result.getString("website_type"), result.getString("ws_0005"), result.getString("website_url"), result.getString("domain_id")))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null)
        conn.close()
    }
    websiteMap
  }

  def findWebSiteByDomain(main_domain: String, webSiteBroadcast: Broadcast[HashMap[String, Website]]): Website = {
    var index: Int = -1
    var info = Website("-1", "-1", "-1", "-1", "-1", "-1")
    if (main_domain != "-1") {
      val webInfo = webSiteBroadcast.value
      if (!webInfo.get(main_domain).isEmpty) {
        info = webInfo.get(main_domain).get
      }
    }
    info
  }

  def getWebChannel:  HashMap[String, WebChannel]  ={
    var conn: Connection = null
    val re = HashMap[String, WebChannel]()
    println("connect to mysql :BigFile 233333 XD")
    try {
      //conn = DriverManager.getConnection("jdbc:mysql://10.0.30.14:3306/rmp?useUnicode=true&characterEncoding=utf-8", "root", "qwertyu")
      conn = JdbcUtil.getConnection()
      val statement = conn.createStatement()
      val result = statement.executeQuery("""
      SELECT website_name,channel_id,channel_name,`name`,perfix,extname from rm_bigfile_website_manager WHERE `status`=1""")
      while (result.next()) {
        re.put(result.getString("website_name"), WebChannel(result.getString("website_name"), result.getString("channel_id"), result.getString("channel_name"),
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

  def findWebChannelByName(main_domain: String,url:String, webSiteBroadcast: Broadcast[HashMap[String, WebChannel]]): WebChannel ={
    //var info: (String, String, String, String, String, String) = ("-1", "-1", "-1", "-1", "-1", "-1")
    var result=WebChannel("-1", "-1", "-1", "-1", "-1", "-1")
    //info:website_name,channel_id,channel_name,name,perfix,extname
    if (main_domain != "-1") {
      val webInfo = webSiteBroadcast.value
      if (!webInfo.get(main_domain).isEmpty) {
        var info = webInfo.get(main_domain).get
        if(info.name.contains(url)&&info.perfix.contains(url)&&info.extname.contains(url)){
          result=info
        }
      }
    }
    result
  }

  def insertDataCheckLog(checkLog:CheckLog) ={
    var result = 0
    var conn:Connection = null
    try{
      conn = getConnection(Config.config_jdbc_param.driver,Config.config_jdbc_param.apollo_url,Config.config_jdbc_param.apollo_username,Config.config_jdbc_param.apollo_password)
      val sql =
        """
          INSERT INTO data_check_log (
          	data_path,
          	data_date,
          	state,
          	correct_ratio,
          	start_wf_ratio,
          	data_correct_path,
          	data_log_path,
          	start_time,
          	end_time,
          	source_name,
          	load_wf_id,
          	company,
          	coll_code,
          	check_detail
          )
          VALUES
          	(
          		?,?,?,?,?,?,?,?,?,?,?,?,?,?
          	)
        """
      val statement = conn.prepareStatement(sql)
      statement.setString(1,null)
      statement.setString(2,checkLog.etlDate)
      statement.setString(3,checkLog.state)
      statement.setDouble(4,checkLog.correct_ratio)
      statement.setDouble(5,0)
      statement.setString(6,"correct")
      statement.setString(7,"log")
      statement.setString(8,checkLog.startTime)
      statement.setString(9,checkLog.endTime)
      statement.setString(10,"DNS_LOG")
      statement.setInt(11,500)
      statement.setString(12,null)
      statement.setString(13,checkLog.province_code)
      statement.setString(14,checkLog.message)
      result = statement.executeUpdate()
    }catch {
      case ex:Exception => ex.printStackTrace()
    }finally {
      if(conn!=null)
        conn.close()
    }
    result
  }

  def updateloadDataWfLog(schedule_state:String,id:Int) = {
    var result = 0
    var conn:Connection = null
    try{
      conn = getConnection(Config.config_jdbc_param.driver,Config.config_jdbc_param.apollo_url,Config.config_jdbc_param.apollo_username,Config.config_jdbc_param.apollo_password)
      val sql = "update load_data_wf_log set state='"+schedule_state+"' where id="+id
      val statement = conn.prepareStatement(sql)
      result = statement.executeUpdate()
    }catch {
      case ex:Exception => ex.printStackTrace()
    }finally {
      if(conn!=null)
        conn.close()
    }
    result
  }

  def insert_parse_code_ratio(map:mutable.HashMap[String,Long]): Unit ={
    var result = 0
    var conn:Connection = null
    try{
      conn = getConnection(url=Config.config_jdbc_param.jx_dns_url)
      var sql = "INSERT INTO dm_parse_code_ratio (rate_type,parse_number,etldate,update_date) VALUES"

      val it = map.iterator
      val etldate = DateUtil.getEtlDate()
      val currentTime = DateUtil.getCurrentDateTime()
      while(it.hasNext){
        val (k,v) = it.next()
        var part = s"('$k',$v,${etldate.toInt},'$currentTime'),"
        sql+=part
      }
      sql = sql.substring(0,sql.length-1)
      val statement = conn.prepareStatement(sql)
      result = statement.executeUpdate()
    }catch {
      case ex:Exception => ex.printStackTrace()
    }finally {
      if(conn!=null)
      conn.close()
    }
    result
  }

  def main(args: Array[String]): Unit = {
    JdbcUtil.insert_parse_code_ratio(mutable.HashMap("a"->1))
  }

}
