package cn.ac.iie.spark.streaming.scala.base
import java.text.SimpleDateFormat

import scala.util.matching.Regex

import cn.ac.iie.spark.streaming.java.GetDomainInfo
import cn.ac.iie.spark.streaming.java.GetFileInfo
import cn.ac.iie.spark.streaming.util.CodeTools
import cn.ac.iie.spark.streaming.util.DateTools
import cn.ac.iie.spark.streaming.util.IpTools
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

//case class CacheLogBean(fileType: String, date: Long, deviceIp: String, userIp: String, serverIp: String, userType: String, city: String, operator: String, system: String, httpMethod: String, httpVersion: String, host: String, mainDomain: String, uri: String, urlType: String, userAgent: String, TerminalType: String, referer: String, contentType: String, statusCode: String, codeSummary: String, cacheState: String, stateSummary: String, cacheHost: String, cacheFlow: String, requestStartDate: String, requestEndDate: String, CacheTimeValue: Long, firstResponseTime: Long, URLHashCode: String, cacheType: String, backSourceIP: String, backTargetIP: String, backHttpMethod: String, backHttpVersion: String, backHost: String, backUri: String, backContentType: String, backStateCode: String, backCodeSummary: String, backCloseState: String, backCacheControl: String, backMaxGge: String, backFileSize: String, backSaveState: String, backServerPort: String, backFlow: Long, backRequestStartDate: String, backRequestEndDate: String, backTimeValue: Long, backFirstResponseTime: Long, requestNum: Long)

/*case class CacheAccessBean(
  fileType: String,
  date: Long,
  //  date: String,
  deviceIp: String,
  //  userIp: String,
  serverIp: String,
  userType: String,
  city: String,
  operator: String,
  system: String,
  httpMethod: String,
  httpVersion: String,
  host: String,
  mainDomain: String,
  uri: String,
  urlType: String,
  userAgent: String,
  TerminalType: String,
  referer: String,
  contentType: String,
  statusCode: String,
  codeSummary: String,
  cacheState: String,
  cacheStateSummary: String,
  cacheHost: String,
  cacheFlow: String,
  requestStartDate: String,
  requestEndDate: String,
  CacheTimeValue: Long,
  firstResponseTime: Long,
  URLHashCode: String,
  cacheType: String,
  //  backSourceIP: String,
  backTargetIP: String,
  backHttpMethod: String,
  backHttpVersion: String,
  backHost: String,
  backUri: String,
  backContentType: String,
  backState: String,
  backStateSummary: String,
  backCloseState: String,
  backCacheControl: String,
  backMaxGge: String,
  backFileSize: String,
  backSaveState: String,
  backServerPort: String,
  backFlow: Long,
  backRequestStartDate: String,
  backRequestEndDate: String,
  backTimeValue: Long,
  backFirstResponseTime: Long,
  requestNum: Long)*/
case class CacheAccessBean(
  fileType: String,
  date: Long,
  //  date: String,
  deviceIp: String,
  userIp: String,
  serverIp: String,
  httpMethod: String,
  httpVersion: String,
  host: String,
  userAgent: String,
  statusCode: String,
  cacheState: String,
  cacheFlow: String,
//  requestStartDate:String,      
//  requestEndDate:String,
  CacheTimeValue:Long,
  firstResponseTime:Long,
  cacheType: String,
  backSourceIP: String,
  backTargetIP: String,
  backHttpMethod: String,
  backHttpVersion: String,
  backHost: String,
  backStateCode: String,
  backCloseState: String,
  backFlow: Long,
//  backRequestStartDate:String,      
//  backRequestEndDate:String,
  backTimeValue: Long,
  backFirstResponseTime: Long,
  requestNum: Long)
//64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
//2017/06/05 01:35:01|117.169.4.169|117.164.251.211|117.169.4.169|GET|HTTP/1.1|s4.qhimg.com|/!01fc2b8a/check.css|Mozilla/5.0 (Linux; U; Android 2.2; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1|NULL|text/css|200|HIT|6510|3|2017/06/05 01:35:01.000|2017/06/05 01:35:01.000|2017/06/05 01:35:01.386|NULL|0|117.169.4.169|NULL|NULL|NULL|NULL|NULL|NULL|NULL|0|cache|315360000|3|1|NULL|0|NULL|NULL|NULL
object CacheAccessLog {

  val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  // Apache日志的正则
  val PARTTERN: Regex = """^(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2})\|(\d+\.\d+\.\d+\.\d+)\|(\d+\.\d+\.\d+\.\d+)\|(\S*)\|(\S+)\|(\S*)\|(\S+)\|(\S+)\|([\S+\s+]+)\|(\S*)\|([\S+\s+]*)\|(\d{3})\|(\S+)\|(\S*)\|(\d+)\|(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}.\d{3})\|(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}.\d{3})\|(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}.\d{3})\|(\S*)\|(\S*)\|(\d+\.\d+\.\d+\.\d+)\|(\S*)\|(\S*)\|(\S*)\|(\S*)\|([\S+\s+]*)\|([\S+\s+]*)\|(\S*)\|(\S*)\|(\S*)\|(\S*)\|(\d*)\|(\S*)\|(\S*)\|(\d+)\|([\S+\s+]*)\|([\S+\s+]*)\|([\S+\s+]*)""".r
  /**
   * 验证一下输入的数据是否符合给定的日志正则，如果符合返回true；否则返回false
   *
   * @param line
   * @return
   */
  def isValidateLogLine(line: String): Boolean = {
    val options = PARTTERN.findFirstMatchIn(line)
    //         val matcher = options.get
    //         println(matcher.group(1)+"==="+matcher.group(2)+"==="+matcher.group(3)+"==="+matcher.group(4)+"==="+matcher.group(5)+"==="+matcher.group(6)+"==="+matcher.group(7)+"==="+matcher.group(8)+"==="+matcher.group(9)+"==="+matcher.group(10)+"==="+matcher.group(11)+"==="+matcher.group(12)+"==="+matcher.group(13)+"==="+matcher.group(14)+"==="+matcher.group(15)+"==="+matcher.group(16)+"==="+matcher.group(17)+"==="+matcher.group(18)+"==="+matcher.group(19)+"==="+matcher.group(20)+"==="+matcher.group(21)+"==="+matcher.group(22)+"==="+matcher.group(23)+"==="+matcher.group(24)+"==="+matcher.group(25)+"==="+matcher.group(26)+"==="+matcher.group(27)+"==="+matcher.group(28)+"==="+matcher.group(29)+"==="+matcher.group(30)+"==="+matcher.group(31)+"==="+matcher.group(32)+"==="+matcher.group(33)+"==="+matcher.group(34)+"==="+matcher.group(35)+"==="+matcher.group(36)+"==="+matcher.group(37)+"==="+matcher.group(38))
    if (options.isEmpty) {
      false
    } else {
      true
    }
  }
  //  val sparkContext = SparkContextSingleton.getInstance(sparkConf)
  //  val ipRulesArray = IpTools.getIPControl()
  //  val ipRulesBroadcast = sparkContext.broadcast(ipRulesArray)
  //  val WebinfoArray = WebSiteInfo.getWebSiteInfo
  //  val webSiteBroadcast = sparkContext.broadcast(WebinfoArray)
  val domaininfo = new GetDomainInfo();
  val fileinfo = new GetFileInfo();
  /**
   * 解析输入的日志数据
   *
   * @param line
   * @return
   */
//  def parseLogLine(line: String, ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]], webSiteBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]]): CacheAccessBean = {

      def parseLogLine(line: String): CacheAccessBean = {
    //    if (!isValidateLogLine(line)) {
    //      throw new IllegalArgumentException("参数格式异常")
    //    }
//    if (isValidateLogLine(line)) {
      // 从line中获取匹配的数据
      val options = PARTTERN.findFirstMatchIn(line)
      // 获取matcher
      val matcher = options.get
      CacheAccessBean(
        "small", //fileType
        DateTools.getFiveTime(matcher.group(1)), // date 
        //      DateTools.getFiveDate(matcher.group(1)), // date
        matcher.group(2), // deviceIp             
        matcher.group(3), // userIp  
        matcher.group(4), // serverIp 
        matcher.group(5), // httpMethod           
        matcher.group(6), // httpVersion          
        matcher.group(7), // host
        matcher.group(9), // userAgent                   
        matcher.group(12), // statusCode
        matcher.group(13), // cacheState           
        matcher.group(15), // cacheFlow
//        matcher.group(16), // requestStartDate      
//        matcher.group(17), // requestEndDate
        DateTools.getTimeValue(matcher.group(16), matcher.group(17)), //CacheTimeValue
        DateTools.getTimeValue(matcher.group(16), matcher.group(18)), //firstResponseTime         
        matcher.group(20), // cacheType             
        matcher.group(21), // backSourceIP          
        matcher.group(22), // backTargetIP          
        matcher.group(23), // backHttpMethod        
        matcher.group(24), // backHttpVersion       
        matcher.group(25), // backHost                   
        matcher.group(28), // backStateCode
        matcher.group(29), // backCloseState        
        matcher.group(35).toLong, // backFlow 
//        matcher.group(36), // backRequestStartDate  
//        matcher.group(37), // backRequestEndDate
        DateTools.getTimeValue(matcher.group(36), matcher.group(37)), //backTimeValue
        DateTools.getTimeValue(matcher.group(36), matcher.group(38)), //backFirstResponseTime
        1)
      // 构建返回值
      /* CacheAccessBean(
      "small", //fileType
      DateTools.getFiveTime(matcher.group(1)), // date 
      //      DateTools.getFiveDate(matcher.group(1)), // date
      matcher.group(2), // deviceIp             
      //      matcher.group(3), // userIp  
      matcher.group(4), // serverIp  
      IpTools.getIPBelong(matcher.group(3), ipRulesBroadcast)._7,
      IpTools.getIPBelong(matcher.group(21), ipRulesBroadcast)._4,
      IpTools.getIPBelong(matcher.group(21), ipRulesBroadcast)._5,
      IpTools.getIPBelong(matcher.group(21), ipRulesBroadcast)._6,
      matcher.group(5), // httpMethod           
      matcher.group(6), // httpVersion          
      matcher.group(7), // host
      domaininfo.getDomainInfo(matcher.group(7), "domain"), //mainDomain
      matcher.group(8), // uri
      fileinfo.getUrlType(matcher.group(8)), //urlType
      matcher.group(9), // userAgent
      fileinfo.getUserAgent(matcher.group(9)), //TerminalType
      matcher.group(10), // referer               
      matcher.group(11), // contentType           
      matcher.group(12), // statusCode
      CodeTools.getCodeSummary(matcher.group(12)), //codeSummary
      matcher.group(13), // cacheState
      CodeTools.getCacheState(matcher.group(13)), //cacheCodeSummary
      matcher.group(14), // cacheHost             
      matcher.group(15), // cacheFlow             
      matcher.group(16), // requestStartDate      
      matcher.group(17), // requestEndDate
      DateTools.getTimeValue(matcher.group(16), matcher.group(17)), //CacheTimeValue
      DateTools.getTimeValue(matcher.group(16), matcher.group(18)), //firstResponseTime
      //      matcher.group(18), // firstResponseTime     
      matcher.group(19), // URLHashCode           
      matcher.group(20), // cacheType             
      //      matcher.group(21), // backSourceIP          
      matcher.group(22), // backTargetIP          
      matcher.group(23), // backHttpMethod        
      matcher.group(24), // backHttpVersion       
      matcher.group(25), // backHost              
      matcher.group(26), // backUri               
      matcher.group(27), // backContentType       
      matcher.group(28), // backState
      CodeTools.getCodeSummary(matcher.group(28)), //backCodeSummary
      matcher.group(29), // backCloseState        
      matcher.group(30), // backCacheControl      
      matcher.group(31), // backMaxGge            
      matcher.group(32), // backFileSize          
      matcher.group(33), // backSaveState         
      matcher.group(34), // backServerHost        
      matcher.group(35).toLong, // backFlow              
      matcher.group(36), // backRequestStartDate  
      matcher.group(37), // backRequestEndDate
      DateTools.getTimeValue(matcher.group(36), matcher.group(37)), //backTimeValue
      DateTools.getTimeValue(matcher.group(36), matcher.group(38)), //backFirstResponseTime
      //      matcher.group(38)) // backFirstResponseTime 
      1L)*/
//    }else{
//      null
//    }
  }

  def main(args: Array[String]) {
    //    val line = """2017/06/05 01:35:01|117.169.4.169|117.164.251.211|117.169.4.169|GET|HTTP/1.1|s4.qhimg.com|/!01fc2b8a/check.css|Mozilla/5.0 (Linux; U; Android 2.2; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1|NULL|text/css|200|HIT|6510|3|2017/06/05 01:35:01.000|2017/06/05 01:35:01.000|2017/06/05 01:35:01.386|NULL|0|117.169.4.169|NULL|NULL|NULL|NULL|NULL|NULL|NULL|0|cache|315360000|3|1|NULL|0|NULL|NULL|NULL"""
    //    val line="""64.242.88.10"""
    //    val line="""2017/06/05 06:30:01|117.169.86.27|39.168.115.51|117.169.86.27|GET|HTTP/1.1|183.250.178.150|/vhot2.qqvideo.tc.qq.com/Al09KtHKApHL_5EAdGrI_1ZOsq-hzbu_-blHtyvbuccs/m0360m4ehtq.mp4?vkey=DDCAF149DA1ED4F2B35EED695024479530A3A7382E19E6F3E600F113D8DBBEF093A44C1CDE4652F2B1A4D6E2A7110F09D63D09765EC91FF58DB17091A3D4449332156ABD286C3E1F30341637D2245AA4992D5C81A2064FB0&sdtfrom=v3060&type=mp4&platform=60401&fmt=mp4&level=0&br=60&sp=0&locid=e2f295f2-1a98-42d6-b06c-7ae1653f55e0&size=17007718&ocid=2698974636|Mozilla/4.0 (compatible; MSIE 5.00; Windows 98)|NULL|video/mp4|206|MISS|6510|97832|2017/06/05 06:28:52.000|2017/06/05 06:30:01.000|2017/06/05 06:28:52.942|NULL|NULL|117.169.86.27|183.250.178.150|GET|HTTP/1.1|183.250.178.150|/vhot2.qqvideo.tc.qq.com/Al09KtHKApHL_5EAdGrI_1ZOsq-hzbu_-blHtyvbuccs/m0360m4ehtq.mp4?vkey=DDCAF149DA1ED4F2B35EED695024479530A3A7382E19E6F3E600F113D8DBBEF093A44C1CDE4652F2B1A4D6E2A7110F09D63D09765EC91FF58DB17091A3D4449332156ABD286C3E1F30341637D2245AA4992D5C81A2064FB0&sdtfrom=v3060&type=mp4&platform=60401&fmt=mp4&level=0&br=60&sp=0&locid=e2f295f2-1a98-42d6-b06c-7ae1653f55e0&size=17007718&ocid=2698974636|video/mp4|200|0|cache|7200|524288|1|80|17007718|2017/06/05 06:28:52.000|2017/06/05 06:30:01.000|2017/06/05 06:28:52.825"""
    ////            val line="""2017/06/05 01:34:56|117.169.4.169|100.90.197.145|117.169.4.169|GET|HTTP/1.1|wap.gz91.com|/jobv/index/job7733_id/1501182181/jobs_id/1101102751.html|Mozilla/5.0 (Linux; Android 6.0.1; vivo X9 Build/MMB29M) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/38.0.0.0 Mobile Safari/537.36 VivoBrowser/5.1.4|http://wap.gz91.com/jobs?noc_new=%E5%B4%87%E4%B9%89&lastdate=1494750076&page=7|text/html; charset=utf-8|200|MISS|6510|11867|2017/06/05 01:34:56.000|2017/06/05 01:34:56.000|2017/06/05 01:34:56.464|NULL|NULL|117.169.4.169|114.80.152.199|GET|HTTP/1.1|wap.gz91.com|/jobv/index/job7733_id/1501182181/jobs_id/1101102751.html|text/html; charset=utf-8|200|0|no-cache|0|0|0|80|11880|2017/06/05 01:34:56.000|2017/06/05 01:34:56.000|2017/06/05 01:34:56.464"""
    val line = "2017/06/05 01:35:00|117.169.4.169|100.110.238.226|117.169.4.169|GET|HTTP/1.1|s4.qhimg.com|/!01fc2b8a/check.css|Mozilla/5.0 (Linux; U; Android 2.2; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1|NULL|text/css|200|HIT|6510|3|2017/06/05 01:35:00.000|2017/06/05 01:35:00.000|2017/06/05 01:35:00.971|NULL|0|117.169.4.169|NULL|NULL|NULL|NULL|NULL|NULL|NULL|0|cache|315360000|3|1|NULL|0|NULL|NULL|NULL"
    val flag = isValidateLogLine(line)
    println(flag)
  }
}