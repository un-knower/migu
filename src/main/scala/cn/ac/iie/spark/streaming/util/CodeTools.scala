package cn.ac.iie.spark.streaming.util

object CodeTools {
  def getCodeSummary(code: String): String = {

    val codeSummary = code match {
      case code if code.startsWith("2") => "2xx"
      case code if code.startsWith("3") => "3xx"
      case code if code.startsWith("4") => "4xx"
      case code if code.startsWith("5") => "5xx"
      case _                            => "other"
    }
    codeSummary
  }

  def main(args: Array[String]) {
    val date = ""
    println(getCodeSummary(date))
  }

  def getCacheState(cacheState: String): String = {
    val state=cacheState.toLowerCase()
    val stateSummary = state match {
      case cacheState if cacheState.contains("hit") => "hit"
      case cacheState if cacheState.startsWith("miss") => "miss"
      case _                                        => "other"
    }
    stateSummary
  }
}