package cn.ac.iie.spark.streaming.util

  //将String转换成Int
  private[spark] object IntParam {
  def unapply(str: String): Option[Int] = {
    try {
      Some(str.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }
}