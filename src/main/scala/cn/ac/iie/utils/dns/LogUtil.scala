package cn.ac.iie.utils.dns

object LogUtil {

  def log(log:String): Unit = {
    println(s"${DateUtil.getCurrentDateTime()} $log")
  }


}
