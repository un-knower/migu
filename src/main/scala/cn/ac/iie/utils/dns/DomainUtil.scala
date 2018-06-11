package cn.ac.iie.utils.dns

import java.util.regex.Pattern

import cn.ac.iie.utils.dns.IpUtil.ipRegex

import scala.util.matching.Regex

object DomainUtil {

  val fanDomainRegex = "[^.]*(\\.(com|cn|edu|net|org|biz|info|gov|pro|name|museum|coop|aero|idv|tv|co|tm|lu|me|cc|mobi|fm|sh|la|dj|hu|so|us|hk|to|tw|ly|tl|in|es|jp|mx|vc|io|am|sc|cm|pw|de|sg|cd|fr|arpa|asia|im|tel|mil|jobs|travel))+$".r
  val domainRegex = "^[0-9a-zA-Z]+[0-9a-zA-Z\\.-]*\\.[a-zA-Z]{2,4}$".r
  val userAgentRegex = ".*(android).*?(mobile?).*".r
  val uriTpyeRegex1 = "(avi|rmvb|mpeg|mpg|mov|movie|rm|mp4|flv|f4v|hlv|m4v|letv|pfv|ts|wmv|mkv|m5v)".r
  val uriTpyeRegex2 = "(rar|exe|zip|bz2|z|iso|mpga|ra|wmp|3gp|tar|arj|gzip|gz|ipa|cab|msu|ogg|kprar|aac|tgz|rtp|spk|apk|deb|pak|xazp|patch|rp|package|pcf|gmz|mpq)".r
  val uriTpyeRegex3 = "(mp3|wma|m4a|m4r)".r

  //val cdnDomain  = Array(".akadns.net",".w.kunlunca.com",".gds.alibabadns.com",".gslb.qianxun.com")//cdn域名，在获取泛域名时要过滤掉这些域名

  def getFanDomain(domain:String,regex:Regex=fanDomainRegex)={
    val matches = regex.findFirstIn(domain)
    val result = matches match {
      case Some(s) =>
        val sp = domain.split(s)
        if(sp.length==0){
          (s,1)
        }else{
          val level = sp(0).split("\\.").length+1
          (s,level)
        }
      case None =>(domain,-1)
    }
    result
  }

  def isValidDomain(domain:String): Boolean ={
    val matches = domainRegex.findAllMatchIn(domain)
    if(matches.isEmpty)
      false
    else
      true
  }

  def getUserAgent(userAgent:String)={
    val ua = userAgent.toLowerCase()
    var model = "windows PC"
    if (ua.contains("android")) {
      if (userAgentRegex.findAllMatchIn(ua).isEmpty) {
        model = "安卓pad"
      } else {
        model = "安卓手机（普通浏览器）"
      }
    } else if (ua.contains("fennec")) {
      model = "Android Firefox手机版Fennec"
    } else if (ua.contains("juc")) {
      model = "Android UC For android"
    } else if (ua.contains("iphone")) {
      model = "苹果手机"
    } else if (ua.contains("ipad")) {
      model = "iPad"
    } else if (ua.contains("blackberry")) {
      model = "BlackBerry"
    } else if (ua.contains("nokian97")) {
      model = "NokiaN97"
    } else if (ua.contains("windows phone")) {
      model = "Windows Phone"
    }
    model
  }

  def getUriType(uri:String): String ={
    var uri_type = "0"
    var trim = uri.toLowerCase.trim
    val pos1 = trim.indexOf('?')
    if (pos1 > 0)
      trim = trim.substring(0, pos1)
    var pos2 = trim.lastIndexOf('/')
    if (pos2>=0)
      trim = trim.substring(pos2, trim.length())
    val pos3 = trim.lastIndexOf('.')
    if (pos3 >= 0)
      trim = trim.substring(pos3 + 1)
    if (trim.length() <= 10){
      if (uriTpyeRegex1.findAllMatchIn(trim).nonEmpty) {
        uri_type = "1"
      } else if (uriTpyeRegex2.findAllMatchIn(trim).nonEmpty) {
        uri_type = "2"
      } else if (uriTpyeRegex3.findAllMatchIn(trim).nonEmpty) {
        uri_type = "3"
      }
    }
    uri_type
  }

  def main(args: Array[String]): Unit = {
    println(getUriType("y.gtimg.cn/music/photo_new/T002R300x300M000001y8sYM3Eje1m.zip"))
  }

}

