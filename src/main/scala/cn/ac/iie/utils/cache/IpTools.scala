package cn.ac.iie.utils.cache

import java.sql.{Connection, DriverManager}
import cn.ac.iie.dataImpl.cache.CacheSourceTools
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

object IpTools extends Serializable{

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    //    val pattern = new Regex("(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])")
    //    val pattern ="""((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)""".r
    val pattern = """^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])$""".r
    //    println(pattern.findAllIn(ip).mkString(",") + "=========" + fragments.length)

    var ipNum = 0L
    if (fragments.length == 4) {
      if (!pattern.findAllIn(ip).isEmpty) {
        for (i <- 0 until fragments.length) {
          ipNum = fragments(i).toLong | ipNum << 8L
        }
      }
    }
    ipNum
  }
  def validIP(ip: String): Boolean = {
    var flag = false
    val pattern = """^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])$""".r
    if (!pattern.findAllIn(ip).isEmpty) {
      flag = true
    }
    flag
  }

  def binarySearch(lines: ArrayBuffer[(String, String, String, String, String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      //      println("middle==="+middle+"==========="+ip+"==statnum"+lines(middle)._1+"==endnum==="+lines(middle)._2)
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong)) {
        return middle
      }
      if (ip < lines(middle)._1.toLong) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    -1
  }

  def getIPControl(): ArrayBuffer[(String, String, String, String, String, String, String)] = {
    var conn: Connection = null
    val re = new ArrayBuffer[(String, String, String, String, String, String, String)]()
    println("getIPControl")
    val properties = CacheSourceTools.loadProperties()
    try {
//      conn = DriverManager.getConnection("jdbc:mysql://10.0.30.14:3306/rmp?useUnicode=true&characterEncoding=utf-8", "root", "qwertyu")
      conn = DriverManager.getConnection(properties.getProperty("mysqlConn"), properties.getProperty("mysql.user"),properties.getProperty("mysql.pwd"))
      val statement = conn.createStatement()
      val result = statement.executeQuery("select * from ifp_base_hn.ip_control order by int_startip,int_endip asc")
//      val result = statement.executeQuery("select * from ip_control order by int_startip,int_endip asc")
      while (result.next()) {
        re.append((result.getString("int_startip"), result.getString("int_endip"), result.getString("location"), result.getString("city"), result.getString("operator"), result.getString("system"), result.getString("user_type")))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null)
        conn.close()
    }
    re
  }

  def getIPBelong(ip: String, ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]]): (String, String, String, String, String, String, String) = {
    var index: Int = -1
    
    val ipNum = ip2Long(ip)
    if (ipNum != 0L) index = binarySearch(ipRulesBroadcast.value, ipNum)
    var info: (String, String, String, String, String, String, String) = ("-1", "-1", "-1", "-1", "-1", "-1", "-1")
    if (index != -1) {
      info = ipRulesBroadcast.value(index)
    }
    info
  }

  /*  val data2MySql = (iterator:Iterator[(String,Int)])=>{
    var conn:Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO location_info(location,counts,access_date) values(?,?,?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=utf-8", "root", "123")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }

  }*/

  def main(args: Array[String]) {

    //windows上报错才加的，在linxu上不需要
    //    System.setProperty("hadoop.home.dir","C:\\tianjun\\winutil\\")

    /*  val conf = new SparkConf().setMaster("local[2]").setAppName("IpLocation")
    val sc = new SparkContext(conf)

    //加载ip属地规则（可以从多台数据获取）
    //    val ipRuelsRdd = sc.textFile("D://ip_control.txt").map(line=>{
    //      val i=0
    //      val fields = line.split("\\|")
    //      val start_num =fields(2)
    //      val end_num =fields(3)
    //      val province = fields(4)
    //      val city = fields(5)
    //      val operator = fields(6)
    //      val system = fields(7)
    //      val user_type = fields(8)
    //      (start_num,end_num,province,city,operator,system,user_type)
    //    })

    //全部的ip映射规则
            val ipRulesArray = getIPControl()
    //    println("0000000000000000000====="+ipRulesArray.length)

    //广播规则
            val ipRulesBroadcast = sc.broadcast(ipRulesArray)
      //加载处理的数据
    val ipsRDD = sc.textFile("D://log.txt").map(line=>{
      val fields = line.split("\\|")
      fields(0)
    })
     var index:Int= -1
    val result = ipsRDD.map(ip =>{
      val ipNum = ip2Long(ip)
       index = binarySearch(ipRulesBroadcast.value,ipNum)
       print("222222222222222===="+ipNum+"====index===="+index)
       var info:(String,String,String,String,String,String,String)=("-1","-1","-1","-1","-1","-1","-1")
       if(index != -1){
         info = ipRulesBroadcast.value(index)
       }
       println("====================="+info.toString())
        info    
    })

//    result.foreachPartition(data2MySql)
    println(result.collect().toBuffer)
            val ip = "10.256.256.0.0"
//        println(ip2Long(ip))
            println(getIPBelong(ip,ipRulesBroadcast))
    sc.stop()*/
    val ip = "1.2.3.25"
    println(validIP(ip))
  }
}