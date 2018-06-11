package cn.ac.iie.dataImpl.cache

import java.io.InputStream
import java.net.URL
import java.text.{DecimalFormat, ParseException, SimpleDateFormat}
import java.util.{Calendar, Date, Properties}

import cn.ac.iie.utils.cache.IpTools
import cn.ac.iie.utils.cache.WebSiteInfo
import cn.ac.iie.base.cache.{CacheOverview, MiguCacheOverview, TB_SA_Cache_Small, TB_SA_Cache_big}
import cn.ac.iie.utils.dns.DomainUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
//import org.elasticsearch.spark.sql.EsSparkSQL

import java.io.BufferedInputStream

import cn.ac.iie.spark.streaming.java.GetDomainInfo

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CacheSourceTools {

  //返回请求命中类型
  def getCacheState(cacheState: String): (Int, Int, Int,Int) = {
    val state=cacheState.toUpperCase()
    val stateSummary = state match {
      case cacheState if cacheState.contains("HIT")     => (1,0,0,0)
      case cacheState if cacheState.contains("MISS")  => (0,1,0,0)
      case cacheState if cacheState.contains("DENIED")=> (0,0,1,0)
      case _                                             => (0,0,0,1)//NULL 空 ERROR 都算为：ERROR
    }
    stateSummary
  }
  //获取当前系统时间
  def getNowDate():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.MINUTE,-5)
    var current = dateFormat.format(cal.getTime())
    current
  }

  //test _记录日志写入时间
  def getDateWithInputTime():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.MINUTE,-5)
    var current = dateFormat.format(cal.getTime())
    current
  }

  //精确到分钟的日期格式
  def getNowDateJustmm():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH:mm")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.MINUTE,-5)
    var current = dateFormat.format(cal.getTime())
    current
  }

  //精确到小时的日期格式
  def getNowDateJustHH():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd HH")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.MINUTE,-5)
    var current = dateFormat.format(cal.getTime())
    current
  }

  //返回时分(数据的业务时间)
  def getFiveTime(date: String): String = {
    val sdate = date.split(":")
    val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm")
    if (sdate.length >= 2) {
      val d = sdate(0).toString()
      val split = d.split("-")

      val minute = sdate(1).toInt / 5f
      val mi = minute match {
        case m if m < 1.0  => ":00"
        case m if m < 2.0  => ":05"
        case m if m < 3.0  => ":10"
        case m if m < 4.0  => ":15"
        case m if m < 5.0  => ":20"
        case m if m < 6.0  => ":25"
        case m if m < 7.0  => ":30"
        case m if m < 8.0  => ":35"
        case m if m < 9.0  => ":40"
        case m if m < 10.0 => ":45"
        case m if m < 11.0 => ":50"
        case m if m < 12.0 => ":55"
        case _             => "unkown"
      }
      //                 re=DateTime.parse(d.concat(mi),DateTimeFormat.forPattern("yyyy/MM/dd HH:mm"))
      //      re.append(d).append(mi)
      //val t = df.parse(d.concat(mi)).getTime
      //val t = df.parse(d.concat(mi)).getTime
      var str=split(0).replaceAll("/","")+" "+split(1)
      return str+mi
    }else{
      return "unkown"
    }
  }
  //返回特殊格式的时分秒
  def getFiveDate(date: String): String = {
    val sdate = date.split(":")
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    if (sdate.length == 3) {
      val d = sdate(0).toString()
      val minute = sdate(1).toInt / 5f
      val mi = minute match {
        case m if m < 1.0  => ":00"
        case m if m < 2.0  => ":05"
        case m if m < 3.0  => ":10"
        case m if m < 4.0  => ":15"
        case m if m < 5.0  => ":20"
        case m if m < 6.0  => ":25"
        case m if m < 7.0  => ":30"
        case m if m < 8.0  => ":35"
        case m if m < 9.0  => ":40"
        case m if m < 10.0 => ":45"
        case m if m < 11.0 => ":50"
        case m if m < 12.0 => ":55"
        case _             => "unkown"
      }
      return d+mi
    }
    return   null
  }

  //包含分钟的日期格式
  def getNowDateIncloudmm():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.MINUTE,-5)
    var current = dateFormat.format(cal.getTime())
    //val times: Date = new Date(current)
    //times
    current
  }

  //包含时区的日期格式
  def getUpdateTime(str:String):String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.MINUTE,-5)
    var current = if(str=="")dateFormat.format(cal.getTime())else dateFormat.format(str)
    current+":00"
  }


  //String转Double
  def unapplyToDouble(str: String): (Double) = {
    try {
      str.toDouble
    } catch {
      case e: NumberFormatException => 0
    }
  }
  //单次请求方式的次数(GET/post/其它):(Int,Int,Int)
  def getReqStyle(req:String):(Int,Int,Int)={
    val result = req.toLowerCase() match {
      case code if code.contains("get") => (1,0,0)
      case code if code.contains("post")=> (0,1,0)
      case _                             => (0,0,1)
    }
    result
  }
  //用户终端类型
  def getUserAgent(str:String):String={
    var result = str.toLowerCase match {
      case code if code.contains("android")&&code.contains("mobile")         => "手机"
      case code if code.contains("android")         => "Pad"
      case code if code.contains("fennec")          =>  "手机"
      case code if code.contains("juc")             =>  "手机"
      case code if code.contains("iphone")          =>  "手机"
      case code if code.contains("ipad")            =>  "Pad"
      case code if code.contains("blackberry")     =>  "手机"
      case code if code.contains("nokian97")        => "手机"
      case code if code.contains("windows phone")  =>  "手机"
      case _                                         =>  "PC"
    }
    result
  }
  /**
    *   用于计算webSiteID和webSiteName
    *   ipwebinfoArray（从数据库中读到的WEBIP匹配规则）
    *   根据源文件中的HOST匹配结合ipwebinfoArray匹配出网站信息
    *   返回信息为(15,乐视网,255,语音视频,www.letv.com,letvcloud.com)
    */
  def getWebInfoByRule(str:String,rule: mutable.HashMap[String, (String, String, String, String, String, String)] )={
    var result: (String, String, String, String, String, String)=null
    rule.foreach(line=>{
      if(rule.contains(line._1)){
        result= line._2
      }
    })
    result
  }
  //String=》Int
  def unapply(str: String): Int = {
    try {
      str.toInt
    } catch {
      case e: NumberFormatException => 0
    }
  }
  //返回HTTP命中状态
  def getCodeSummary(code: String): (Int, Int, Int,Int) = {
    if(code==""||code==null){
      (0,0,0,0)
    }else{
      val codeSummary = code match {
      case code if code.startsWith("2") => (1, 0, 0,0)
      case code if code.startsWith("3") => (0, 1, 0,0)
      case code if code.startsWith("4") => (0, 0, 1,0)
      case code if code.startsWith("5") => (0, 0, 0,1)
      case _=>(0,0,0,0)
    }
      codeSummary
    }
  }
  //返回的回源命中状态
  def getSourceSummary(code:String):(Int,Int)={
    val result=code match {
      case code if code.startsWith("4") => (1, 0)
      case code if code.startsWith("5") => (0, 1)
      case _                             => (0, 0)
    }
    result
  }
  //时间日期转换，保留到分钟
  def getDateTrans(time:String):(String)={
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    val parse: Date = df.parse(time)
    parse.toString.trim
  }
  //计算时间差
  def getDate(start_time: String, end_Time: String):(Double)={
    var d_value: Double = 0l
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
    val spcDT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    var end:Long=0L
    try {
      val begin= df.parse(start_time).getTime
      if(end_Time.contains("T")){
        end=spcDT.parse(end_Time).getTime
      }else{
        end=df.parse(end_Time).getTime
      }
      //val end: Date = (end_Time.contains("T")) ? spcDT.parse(end_Time):
      d_value = (end - begin)
      if(d_value<=0){
        d_value=1.0
      }
      return d_value
    } catch {
      case ex: ParseException => 1l
    } finally {
      return d_value
    }
  }
  //计算回源关闭状态(返回DNS解析失败次数、回源建链失败次数)
  def getCloseStatus(status:String):(Int,Int)={
    val codeSummary = status.toUpperCase match {
      case code if code.contains("DNS_ERROR") => (1, 0)
      case code if code.contains("CONNECT_TIMEOUT_2") => (0, 1)
      case code if code.contains("CONNECT_TIMEOUT_3") => (0, 1)
      case code if code.contains("CONNECT_PEER_ERR_1") => (0, 1)
      case code if code.contains("CONNECT_PEER_ERR_2") => (0, 1)
      case _=>(0,0)
    }
    //println("返回类型："+code)
    //println(codeSummary)
    codeSummary
  }

  /*
  *计算地市 IP信息(int_startip,int_endip,location,city,operator,system,user_type)
  */
  def getIPCalculation(str:String,ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]]):(String, String, String, String, String, String, String)={
    val ipNum = IpTools.ip2Long(str)
    var index: Int = -1
    index = IpTools.binarySearch(ipRulesBroadcast.value, ipNum)
    var infoIP: (String, String, String, String, String, String, String) = ("-1", "-1", "-1", "-1", "-1", "-1", "-1")
    var value: (String, String, String, String, String, String, String) = null
    if (index != -1) {
      value = ipRulesBroadcast.value(index)
    }else{
      value=("999","999","999","999","999","999","999")
    }
      value
  }

  /*
    * getCacheSource Entity
    */
  def getCacheSourceEntity(rowID : Int, info_2:String, info_21:String,info_6:String,info_12:String,info_4:String,info_14:String,info_34:String,info_11:String,info_27:String,info_29:String,info_30:String,info_16:String,info_15:String,info_8:String,info_1:String,info_35:String,info_36:String,info_37:String,info_17:String
                           ,ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]]
                           ,ipwebinfoArray:Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]]
                           ,info_28:String
                           ,info_25:String
                           ,cacheType:String
                          ):(CacheOverview)={
    var info_34_value=1.0
    if(unapplyToDouble(info_34)>0){
      info_34_value=info_34.toDouble
    }
    var info_14_value=1.0
    if(unapplyToDouble(info_14)>0){
      info_14_value=info_14.toDouble
    }

    //try {
    //计算地市
    var value= CacheSourceTools.getIPCalculation(info_2.toString.trim,ipRulesBroadcast)
    //计算用户接入方式
    val accessNumb = IpTools.ip2Long(info_21.toString.trim)
    val search = IpTools.binarySearch(ipRulesBroadcast.value, accessNumb)
    var accessType= if(search!= -1)ipRulesBroadcast.value(search) else ("9","9","9","9","9","9","9")
    //Web Info 返回信息为(15,乐视网,255,语音视频,www.letv.com,letvcloud.com)
    val info1 = new GetDomainInfo()
    val webSite = WebSiteInfo.getWebSite(info1.getDomainInfo(info_6.toString,"domain"),ipwebinfoArray)
    //返回命中类型
    val state = CacheSourceTools.getCacheState(info_12.toString)
    //单次请求方式
    val reqStatus: (Int, Int, Int) = CacheSourceTools.getReqStyle(info_4.toString)
    //计算总流量
    var all_flow = 0.0
    val result_AllFlow = info_14_value//info_14.toDouble
    if(result_AllFlow>0){
      all_flow=result_AllFlow*8/1024
    }
    //计算回源流量
    var back_flow=0.0
    var result_backFlow = unapplyToDouble(info_34.toString)
    if(result_backFlow>0){
      back_flow=result_backFlow*8/1024
    }
    //Hit流量
    var hit_flow=if (state._1==1&&all_flow>0) all_flow else 0
    //    var hit_flow=if (state._1==1&&all_flow>0) info_14_value else 0
    //    if(hit_flow!=0){
    //      var a=hit_flow
    //      hit_flow=a*8/1024
    //    }
    //denied流量
    var denied_flow=if (state._3==1&&all_flow>0) all_flow else 0
    //    var denied_flow=if (state._3==1&&result_AllFlow>0) result_AllFlow else 0
    //    if(denied_flow>0){
    //      var b=denied_flow
    //      denied_flow=b*8/1024
    //    }
    //Miss流量
    var miss_flow=if (state._2==1&&all_flow>0) back_flow else 0
    //    var miss_flow=if (state._2==1&&info_34_value>0) info_34_value else 1
    //    if(miss_flow>0){
    //      var a=miss_flow
    //      miss_flow=a*8/1024
    //    }
    //chacheMiss_flow流量
    var chacheMiss_flow=if(state._2==1&&all_flow>0)all_flow else 0

    //返回的HTTP应答Status-Code
    val codeSummary: (Int, Int, Int, Int) = CacheSourceTools.getCodeSummary(info_11)
    //返回的回源Status-Code
    val sourceSummary: (Int, Int) = CacheSourceTools.getSourceSummary(info_27)
    //请求成功
    var reqSuccsess = codeSummary._1 + codeSummary._2
    //请求失败
    var reqFaild = codeSummary._3 + codeSummary._4
    //不应缓存的流量
    var noCacheFlow =if (info_29.toString.contains("No-cache") || info_29.toString.contains("Private") && info_30.toDouble == 0&&info_34_value>0) info_34_value*8/1024 else  0
    //运算：保留两位小数
    val format: DecimalFormat = new DecimalFormat("#.00")
    val numFormat: DecimalFormat = new DecimalFormat("#.0000")
    //计算回源首包时延
    var timeDelay =0
    if(info_35 !=null && info_37 !=null && info_35.toUpperCase.equals("NULL")==false && info_37.toString.toUpperCase.equals("NULL")==false){
      timeDelay=CacheSourceTools.getDate(info_35.toString,info_37.toString).toInt
    }
    //时间
    val date: Date = new Date()
    //平均速率  (流量/（结束时间-开始时间）)          getDate(startTime,endTime)info_16
    val numb1 = CacheSourceTools.getDate(info_16.toString, info_15.toString)//时间
    var speedAvg: Double =0
    if(numb1>0 &&  info_14_value>0){
      speedAvg=(((info_14_value)*8/1024)/numb1)*1000//
    }
    //计算回源下载速率
    var back_timeDelay=0.0
    if(info_35.toString!=null||info_36.toString!=null||info_35.toString.toUpperCase!="NULL"||info_36.toString.toUpperCase!="NULL"){
      back_timeDelay=CacheSourceTools.getDate(info_35.toString,info_36.toString).toDouble
    }
    var back_speed = 0.0//info_34.toDouble/(timeDelay.toDouble*1000)
    if(back_timeDelay!=0){
      back_speed=(info_34.toDouble*8/1024)/(back_timeDelay*1000)
    }

    //计算文件类型
    var file_type="999"
    /*    if(cacheType=="big"){
          //URL信息
          file_type = getUrlInfo(info_25)
        }*/

    //val date1: Date = new Date()
    //format.format(info(14).toInt / (i * 1000)).toInt
    //DNS解析失败次数，回源建链失败次数
    val status: (Int, Int) = getCloseStatus(info_28.toString)
    //var id=rowID+1
    CacheOverview(
      rowID,   //rowID
      "huawei", //厂商
      cacheType, //缓存类型
      info_1.toString, //缓存设备信息
      value._4, //地市
      if(accessType._7=="4") "9" else accessType._7, //接入方式 accessType._7  计算info(21)IP归属信息 重现计算value方法
      CacheSourceTools.getUserAgent(info_8.toString), //用户终端类型
      webSite._1,//webSite._6, //网站ID
      if(webSite._2=="-1") "Unknown" else webSite._2,//webSite._2,//webSite._3, //网站名（中文）
      if(cacheType=="small")info_6 toString else "", //域名
      file_type,//文件类型
      1, //请求次数
      reqSuccsess, //请求成功次数
      reqFaild, //请求失败次数
      state._1, //CacheHit次数
      state._2, //CacheMISS次数
      state._3, //CacheDenied次数
      state._4, //CacheError次数
      codeSummary._1, //2xx次数
      codeSummary._2, //3xx次数
      codeSummary._3, //4xx次数
      codeSummary._4, //5xx次数
      codeSummary._3 + codeSummary._4, //错误代码次数
      reqStatus._1, //Get次数
      reqStatus._2, //Post次数
      reqStatus._3, //其他请求方式次数
      "Kb", //流量计量单位
      if(all_flow>=0) all_flow else 0, //总流量
      hit_flow, //Hit流量
      denied_flow,
      miss_flow, //MISS流量
      chacheMiss_flow,//chacheMiss_flow流量
      format.format(speedAvg).toDouble, //平均下载速率
      sourceSummary._1, //回源4xx错误码次数
      sourceSummary._2, //回源5xx错误码次数
      CacheSourceTools.getDate(info_15.toString,info_17.toString).toInt,//(info(17).toDouble - info(15).toDouble).toInt, //首包时延
      timeDelay,//CacheSourceTools.getDate(info_35.toString,info_37.toString).toInt,//(info(37).toDouble - info(35).toDouble).toInt, //回源首包时延
      status._1, //DNS解析失败次数
      status._2, //回源建链失败次数
      noCacheFlow, //不应缓存的流量
      //0.0,//请求增益比
      //0.0,//流量增益比
      //0.0,//请求MISS率
      back_flow,//回源流量 34
      format.format(back_speed).toDouble,//源站(回源)下载速率
      CacheSourceTools.getUpdateTime(""), //数据更新时间
      "5m", //数据的业务时间频率
      CacheSourceTools.getFiveTime(CacheSourceTools.getNowDateIncloudmm()),//CacheSourceTools.getFiveTime(info_16.toString), CacheSourceTools.getNowDateIncloudmm() //数据的业务时间 bussiness_time
      "-1", //数据的业务时间频率
      "-1" ,//数据的业务时间
      getNowDateJustHH(),
      getNowDate()
    )
  }

  /**
    * 计算 ali CDN 日志
    *
    */

  def getAliCDN(line:Array[String],ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]]
                   ,ipwebinfoArray:Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]]
                  ): MiguCacheOverview ={

    val requestStartDate = line(0)
    val responseDuration = line(3).toLong
    val userIP = line(1)
    val statusCode = line(7)
    val flow = line(9).toDouble*8/1024
    val client_RequestType = line(5)
    val client_RequestURL = line(6)//new URL(line(6))
    val useragent = line(11)
    val province=""
    val user_terminal=getUserAgent(useragent)
    val busines_type=if(client_RequestURL.contains("hlsmgsplive")) "直播" else if(client_RequestURL.contains("hlsmgzblive")) "点播" else "未知"
    val operator=getIPCalculation(userIP,ipRulesBroadcast)._5
    val req=getCodeSummary(statusCode)
    val req_oknum=if(req._1==1||req._2==1) 1 else 0
    val req_fail_num=if(req_oknum==1)0 else 1
    val returnerror_num=if(req_oknum!=0) 0 else 1
    val reqStatus: (Int, Int, Int) = getReqStyle(client_RequestType)

    val domain=new URL(client_RequestURL).getHost
    val back_speed=(flow)/(responseDuration*1000)
    val back4xx_num=if(req._3==1) 1 else 0
    val back5xx_num=if(req._4==1) 1 else 0
    val updateTime = getUpdateTime("")
    val business_time=getBusiness_time(requestStartDate,responseDuration)
    val cacheState = getCacheState(line(10))
    val hit_flow=if(cacheState._1==1) flow else 0
    val miss_flow=if(cacheState._2==1) flow else 0
    val other_flow=if(cacheState._1!=1&&cacheState._2!=1) flow else 0
    MiguCacheOverview("",province,user_terminal,domain,busines_type,operator,1,req_oknum,req_fail_num,cacheState._1,cacheState._2,req._1,req._2,req._3,req._4,returnerror_num,reqStatus._1,reqStatus._2,reqStatus._3,"KB",flow,hit_flow,miss_flow,other_flow,back_speed,back4xx_num,back5xx_num,updateTime,"5m",business_time,getBusiness_time_1h(business_time),getBusiness_time_1d(business_time))
  }

  /**
    * 计算 ali OTT 日志
    *
    */

  def getOTTAnalysis(line:Array[String]
                ,factory:String
                ,ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]]
                ,ipwebinfoArray:Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]]
               ): MiguCacheOverview ={
    val requestStartDate = line(8)
    val responseDuration = line(11).toDouble
    val userIP = line(1)
    val statusCode = ""//line(7)
    val flow = line(12).toDouble
    val client_RequestURL = ""//line(6)
    val client_RequestType = ""//line(5)
    //val useragent = line(11)
    val servicetype=line(6)
    val province=""
    val user_terminal="电视盒子"//getUserAgent(useragent)
    val busines_type=if(servicetype.contains("00")) "点播" else "直播"
    val operator=""//getIPCalculation(userIP,ipRulesBroadcast)._5
    val req=getCodeSummary(statusCode)
    val req_oknum=1//if(req._1==1||req._2==1) 1 else 0
    val req_fail_num=0//if(req_oknum==1)0 else 1
    val returnerror_num=0//if(req_oknum!=0) 0 else 1
    val reqStatus: (Int, Int, Int) = getReqStyle(client_RequestType)

    val domain=""//new URL(client_RequestURL).getHost
    val back_speed=(flow)/(responseDuration)
    val back4xx_num=if(req._3==1) 1 else 0
    val back5xx_num=if(req._4==1) 1 else 0
    val updateTime = getUpdateTime("")
    val business_time=getBusiness_time_oot(line(8))//getBusiness_time(requestStartDate,responseDuration)
    val cacheState = getCacheState(line(10))
    val hit_flow=if(cacheState._1==1) flow else 0
    val miss_flow=if(cacheState._2==1) flow else 0
    val other_flow=if(cacheState._1!=1&&cacheState._2!=1) flow else 0
    MiguCacheOverview("",province,user_terminal,domain,busines_type,operator,1,req_oknum,req_fail_num,cacheState._1,cacheState._2,req._1,req._2,req._3,req._4,returnerror_num,reqStatus._1,reqStatus._2,reqStatus._3,"KB",flow,hit_flow,miss_flow,other_flow,back_speed,back4xx_num,back5xx_num,updateTime,"5m",business_time,getBusiness_time_1h(business_time),getBusiness_time_1d(business_time))
  }



  /**
    * 计算 Yunfan CDN日志
    * @param line
    * @param ipRulesBroadcast
    * @param ipwebinfoArray
    * @return
    */
  def getYunfanCDN(line:Array[String],ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]]
                    ,ipwebinfoArray:Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]]
                   ): MiguCacheOverview={
    val requestStartDate = line(0)
    val responseDuration = line(1).toLong
    val userIP = line(4)
    val statusCode = line(7)
    val flow = line(2).toDouble*8/1024
    //云帆缺失get post内容信息的字段
    val client_RequestType = line(5)
    val client_RequestURL = line(3)
    val useragent = line(5)
    val cacheState = getCacheState(line(8))
    val province=""
    val user_terminal="app"//getUserAgent(useragent)
    val busines_type=if(client_RequestURL.contains("hlsmgzblive.miguvideo.com")||client_RequestURL.contains("hlsmgsplive.miguvideo.com")) "直播" else if(client_RequestURL.contains("zbvod")||client_RequestURL.contains("spvod")) "点播" else "未知"
    val operator="yunfan"//getIPCalculation(userIP,ipRulesBroadcast)._5
    val req=getCodeSummary(statusCode)
    val req_oknum=if(req._1==1||req._2==1) 1 else 0
    val req_fail_num=if(req_oknum==1)0 else 1
    val returnerror_num=if(req_oknum!=0) 0 else 1
    val reqStatus: (Int, Int, Int) = getReqStyle(client_RequestType)

    val domain=new URL(client_RequestURL).getHost
    val back_speed=((flow)/(responseDuration)*1000)
    val back4xx_num=if(req._3==1) 1 else 0
    val back5xx_num=if(req._4==1) 1 else 0
    val updateTime = getUpdateTime("")
    val business_time=getBusiness_time(requestStartDate,responseDuration)
    val hit_flow=if(cacheState._1==1) flow else 0
    val miss_flow=if(cacheState._2==1) flow else 0
    val other_flow=if(cacheState._1!=1&&cacheState._2!=1) flow else 0
    MiguCacheOverview("",province,user_terminal,domain,busines_type,operator,1,req_oknum,req_fail_num,cacheState._1,cacheState._2,req._1,req._2,req._3,req._4,returnerror_num,reqStatus._1,reqStatus._2,reqStatus._3,"KB",flow,hit_flow,miss_flow,other_flow,back_speed,back4xx_num,back5xx_num,updateTime,"5m",business_time,getBusiness_time_1h(business_time),getBusiness_time_1d(business_time))
  }



  /*
   *计算DataFrame
   */
  def getResultDataFrame(inputDF:DataFrame):(DataFrame)={
    val resultDF: DataFrame = inputDF.groupBy(
      "rowid",
      "bussiness_time")
      .agg(
        ("all_num", "count"),
        ("req_ok_num", "count"),
        ("req_fail_num", "count"),
        ("req_Hit_num", "count"),
        ("req_Miss_num", "count"),
        ("req_Denied_num", "count"),
        ("req_Error_num", "count"),
        ("return2xx_num", "count"),
        ("return3xx_num", "count"),
        ("return4xx_num", "count"),
        ("return5xx_num", "count"),
        ("returnerror_num", "count"),
        ("req_get_num", "count"),
        ("req_post_num", "count"),
        ("req_other_num", "count"),
        ("req_Denied_num", "count"),
        ("down_speed", "avg"),
        ("back4xx_num", "count"),
        ("back5xx_num", "count")
      )
    val baseInfo: DataFrame = inputDF.select(
      "rowid",
      "factory",
      "cache_type",
      "cache_eqp_info",
      "city",
      "access_type",
      "user_terminal",
      "webSiteID",
      "webSiteName",
      "domain",
      "flowUnit",
      "all_flow",
      "fastpack_delay",
      "back_fstpack_delay",
      "dnsparse_fail_num",
      "backlink_fail_num",
      "nocache_flow",
      "update_datetime")
    baseInfo.join(resultDF, baseInfo("rowid") === resultDF("rowid"), "left")
  }

  //返回SACacheBigInfo:大文件
  def SACacheBigInfo(lines:Array[String]
                    ,serviceName:String
                     ,ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]]
                     ,ipwebinfoArray:Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]]
                     ,chanelInfoBroadcast:Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]]
                     ): TB_SA_Cache_big ={

    var reqLines: Array[String] = lines.map(line=>{line.replaceAll("'","").replaceAll(",","")})
    //计算地市
    var city=CacheSourceTools.getIPCalculation(reqLines(2).trim,ipRulesBroadcast)
    //计算用户接入方式
    val accessNumb = IpTools.ip2Long(reqLines(21).trim)
    val search = IpTools.binarySearch(ipRulesBroadcast.value, accessNumb)
    var accessType= if(search!= -1)ipRulesBroadcast.value(search) else ("9","9","9","9","9","9","9")
    //Web Info 返回信息为(15,乐视网,255,语音视频,www.letv.com,letvcloud.com)
    val info1 = new GetDomainInfo()
    val webSite = WebSiteInfo.getWebSite(info1.getDomainInfo(reqLines(6),"domain"),ipwebinfoArray)
    //URL信息
    val urlInfo = getUrl(reqLines(25))
    //Channel info
    val webChanel = WebSiteInfo.getWebChanel(webSite._6,urlInfo._1,chanelInfoBroadcast)
    TB_SA_Cache_big(
      //日志记录时间
//      reqLines(0),
      getDateWithInputTime,
      //缓存设备信息
      reqLines(1),
      //用户IP地址
      reqLines(2),
      //用户请求IP地址
      reqLines(3),
      //HTTP Method
      reqLines(4),
      //HTTP Version
      reqLines(5),
      //HTTP Host
      reqLines(6),
      //HTTP Uri
      reqLines(7),
      //HTTP User-agent
      reqLines(8),
      //HTTP Referer
      reqLines(9),
      //HTTP Content-type
      reqLines(10),
      //HTTP Status-code
      reqLines(11),
      //缓存命中状态
      reqLines(12),
      //缓存服务端口
      reqLines(13),
      //缓存吐出流量
      reqLines(14),
      //请求开始时间
      reqLines(15),
      //请求结束时间
      reqLines(16),
      //首字节响应时间
      reqLines(17),
      //URL Hash值
      reqLines(18),
      //缓存命中类型
      reqLines(19),
      //回源源IP地址
      reqLines(20),
      //回源目的IP地址
      reqLines(21),
      //回源HTTP Method
      reqLines(22),
      //回源HTTP Version
      reqLines(23),
      //回源HTTP Host
      reqLines(24),
      //回源HTTP Uri
      reqLines(25),
      //回源HTTP Content-type
      reqLines(26),
      //回源HTTP Status-code
      reqLines(27),
      //回源关闭状态
      reqLines(28),
      //回源Cache-Control
      reqLines(29),
      //回源Max-age
      reqLines(30),
      //回源文件大小
      reqLines(31),
      //缓存保存状态
      reqLines(32),
      //回源服务端口
      reqLines(33),
      //回源流量
      reqLines(34),
      //回源请求开始时间
      reqLines(35),
      //回源请求结束时间
      reqLines(36),
      //源站首字节响应时间
      reqLines(37),
      //厂商:已确认，暂时定为："华为"
      "huawei",
      //缓存类型
      "big",
      //地市
      city._4,
      //接入方式
      accessType._7,
      //用户终端类型
      CacheSourceTools.getUserAgent(reqLines(8)),
      //网站ID
      webSite._1,
      //网站名（中文）
      if(webSite._2=="-1") "Unknown" else webSite._2,
      //泛域名
      webSite._6,
      //文件类型
      urlInfo._2,
      //省份
      "330000",
      //chanel_id
      webChanel._2,
      //chanel_name
      webChanel._3,
      //url后缀
      urlInfo._1
    )
  }

  //返回SACacheInfo:小文件
  def getSACacheInfo(lines:Array[String]
                     ,factory:String
                     ,ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]]
                     ,ipwebinfoArray:Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]]
                    ): TB_SA_Cache_Small ={
    var reqLines: Array[String] = lines.map(line=>{line.replaceAll("'","").replaceAll(",","")})
    //计算用户接入方式
    val accessNumb = IpTools.ip2Long(reqLines(21).trim)
    val search = IpTools.binarySearch(ipRulesBroadcast.value, accessNumb)
    var accessType= if(search!= -1)ipRulesBroadcast.value(search) else ("9","9","9","9","9","9","9")
    //地市
    var value= CacheSourceTools.getIPCalculation(reqLines(2).trim,ipRulesBroadcast)
    //Web Info 返回信息为(15,乐视网,255,语音视频,www.letv.com,letvcloud.com)
//    var info1 = new GetDomainInfo()
//    var domainInfo: String = info1.getDomainInfo(reqLines(6),"domain")
    var domainInfo= DomainUtil.getFanDomain(reqLines(6))
    var webSite = WebSiteInfo.getWebSite(domainInfo._1,ipwebinfoArray)

    TB_SA_Cache_Small(
//     reqLines(0),
    getDateWithInputTime,
     reqLines(1),
     reqLines(2),
     reqLines(3),
     reqLines(4),
     reqLines(5),
     reqLines(6),
     reqLines(7),
     reqLines(8),
     reqLines(9),
     reqLines(10),
     reqLines(11),
     reqLines(12),
     reqLines(13),
     reqLines(14),
     reqLines(15),
     reqLines(16),
     reqLines(17),
     reqLines(18),
     reqLines(19),
     reqLines(20),
     reqLines(21),
     reqLines(22),
     reqLines(23),
     reqLines(24),
     reqLines(25),
     reqLines(26),
     reqLines(27),
     reqLines(28),
     reqLines(29),
     reqLines(30),
     reqLines(31),
     reqLines(32),
     reqLines(33),
     reqLines(34),
     reqLines(35),
     reqLines(36),
     reqLines(37),
      factory,
     "small",
      value._4,
      accessType._7,
      CacheSourceTools.getUserAgent(reqLines(8)),
      webSite._1,//webSite._6, //网站ID
      if(webSite._2=="-1") "Unknown" else webSite._2,//webSite._2,//webSite._3, //网站名（中文）
      domainInfo._1,
     getContentType(reqLines(34))
    )
  }
  //获取文件类型
  def getContentType(code:String): String ={
    val result=code match {
      case code if code.contains("application") => "application"
      case code if code.contains("audio") => "audio"
      case code if code.contains("image") => "image"
      case code if code.contains("video") => "video"
      case code if code.contains("text")  => "text"
      case _                               => "other"
    }
    result
  }
  def setInfoToESByHour(str:String): Unit ={


  }

  /*
   * 加载配置文件
   * 传入参数()
   * 返回参数()
   */
  def loadProperties():(Properties) = {
    try {
      val prop: Properties = new Properties()
      //jar包下的class相对路径
      val asStream: InputStream = getClass.getResourceAsStream("/CacheOverView.properties")
      prop.load(new BufferedInputStream(asStream))
      //var resource = CacheSourceTools.getClass.getClassLoader.getResource("CacheOverView.properties")
      //var propConfig:PropertiesConfiguration=null
      //propConfig=new PropertiesConfiguration()
      //"com/neteast/spark/config/CacheOverView.properties"
      //设置自动冲加载机制：每次调用方法，自动加载配置文件
//      propConfig.setReloadingStrategy(new FileChangedReloadingStrategy)
//      prop.load(new FileInputStream("D:/Users/zhangmin/workspace_scala/Hours/src/main/scala/com/neteast/spark/config/CacheOverView.properties"))
      if(prop!=null){
        prop
      }else{
        null
      }
      //val in=//读取键为user.name的数据的值
      //if (in != null) {
        //prop.load(in)
        //println(prop.getProperty("user.name"))
      //}
      //prop
    }catch { case e: Exception =>
      e.printStackTrace()
      println("配置文件加载失败")
      sys.exit(1)
    }
  }

  //缓存日志大文件：URL信息
  def getUrl(urlStr:String): (String,String) ={
    var str=""
    var url_type="other"
    var pos1 = urlStr.length
    val trimStr = urlStr.toLowerCase().trim()
    if(trimStr!=""&&trimStr!="null"){
      if (trimStr.lastIndexOf('/') >= 0) pos1 = trimStr.lastIndexOf('/')
      var url_1 = trimStr.substring(pos1 + 1, trimStr.length())
      if (url_1.lastIndexOf('.') >= 0) str = url_1.substring(url_1.lastIndexOf('.') + 1)
      if (str.length() > 10)
      str = ""
      import java.util.regex.Pattern
      if (Pattern.matches("(rar|zip|exe|mp3|wma|m4a|aac|3gp|cab|apk|ipa|pak|xazp|xy|patch|rp|package|gmz|flac|MPQ|bin|xdt|bz2|z|iso|mpga|ra|rm|wmp|tar|arj|gzip|msu|kprar|ogg|tgz|gz|rtp|spk|7z|ttf|vcdiff|qyswf|psf|gpk|jar|amz|lha|ifs|001|002|003|004|005|006|007|rpk|sus|dmg|pp|esd|wdf|scel|m2t|sjna|tbs|dzp)", str)) url_type = "download"
      else if (Pattern.matches("(flv|mp4|f4v|hlv|wmv|mkv|m5v|avi|rmvb|mpeg|mpg|mov|movie|ts|265ts)", str)) url_type = "video"
      (str,url_type)
    }
    ("other","other")
  }

  //缓存日志全局概览：URL信息
  def getUrlInfo(urlStr:String): String ={
    var str=""
    var url_type="other"
    var pos1 = urlStr.length
    val trimStr = urlStr.toLowerCase().trim()
    if(trimStr!=""&&trimStr!="null"){
      if (trimStr.lastIndexOf('/') >= 0) pos1 = trimStr.lastIndexOf('/')
      var url_1 = trimStr.substring(pos1 + 1, trimStr.length())
      if (url_1.lastIndexOf('.') >= 0) str = url_1.substring(url_1.lastIndexOf('.') + 1)
      if (str.length() > 10) str = ""
      import java.util.regex.Pattern
      if (Pattern.matches("(rar|zip|exe|mp3|wma|m4a|aac|3gp|cab|apk|ipa|pak|xazp|xy|patch|rp|package|gmz|flac|MPQ|bin|xdt|bz2|z|iso|mpga|ra|rm|wmp|tar|arj|gzip|msu|kprar|ogg|tgz|gz|rtp|spk|7z|ttf|vcdiff|qyswf|psf|gpk|jar|amz|lha|ifs|001|002|003|004|005|006|007|rpk|sus|dmg|pp|esd|wdf|scel|m2t|sjna|tbs|dzp)", str))
      {
        url_type = "xiazai"
      }
      else if (Pattern.matches("(flv|mp4|f4v|hlv|wmv|mkv|m5v|avi|rmvb|mpeg|mpg|mov|movie|ts|265ts)", str))
       {
         url_type = "shipin"
       }
      url_type
    }else{
      "qita"
    }
  }

  def getBusiness_time(str:String,millis:Long): String ={
    var format=new SimpleDateFormat("yyyyMMddHHmmSS")//指定日期格式
    val date=format.parse(str)
    val time = date.getTime
    var dataFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")//指定日期格式
    getFiveDate(dataFormat.format(new Date(time+millis)))
  }

  def getBusiness_time_oot(str:String): String ={
    var trans=new SimpleDateFormat("yyyyMMddHHmmss")
    var format=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    getFiveDate(format.format(trans.parse(str)))
  }

  def getBusiness_time_1h(str:String): String ={
    var format=new SimpleDateFormat("yyyy/MM/dd HH")
    format.format(format.parse(str))
  }

  def getBusiness_time_1d(str:String): String ={
    var format=new SimpleDateFormat("yyyy/MM/dd")
    format.format(format.parse(str))
  }

  def main(args: Array[String]): Unit = {
    println(getBusiness_time_oot("20180601001417"))
    println(getBusiness_time("20180601001417",1000))
  }

}
