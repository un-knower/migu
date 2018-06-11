package cn.ac.iie.utils.dns

object IpUtil extends Serializable{

  val ipRegex = "^(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])$".r

  def ip2long(ip: String)= {
    val sp = ip.split("\\.")
    assert(sp.length==4)
    var ipNum = 0L
    for(i <- sp){
      ipNum = i.toLong|ipNum<<8
    }
    ipNum
  }

  def isValidIp(ip:String): Boolean ={
    val matches = ipRegex.findAllMatchIn(ip)
    if(matches.isEmpty)
      false
    else
      true
  }

  def main(args: Array[String]): Unit = {
    println(isValidIp("192.168.5."))
  }
}