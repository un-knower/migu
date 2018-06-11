package cn.ac.iie.spark.streaming.util

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.text.DateFormat
import org.codehaus.jackson.map.util.ISO8601DateFormat

object DateTools {
  def getFiveTime(date: String): Long = {
    val sdate = date.split(":")
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm")
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
//                 re=DateTime.parse(d.concat(mi),DateTimeFormat.forPattern("yyyy/MM/dd HH:mm"))
      //      re.append(d).append(mi)
      val t = df.parse(d.concat(mi)).getTime
      return t
    }
    return 0l
  }

  def getTimeValue(start_time: String, end_Time: String): Long = {
    var d_value: Long = 0l
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
    try {
      val begin: Date = df.parse(start_time)
      val end: Date = df.parse(end_Time)
      d_value = (end.getTime() - begin.getTime())
      return d_value
    } catch {
      case ex: ParseException => 0l
    } finally {
      return d_value
    }
  }
  def getNowDate():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.MINUTE,-5)
    var current = dateFormat.format(cal.getTime())
    current
  }
  
  
  /*
   *         case m if m < 1.0  => ":00:00.000"
        case m if m < 2.0  => ":05:00.000"
        case m if m < 3.0  => ":10:00.000"
        case m if m < 4.0  => ":15:00.000"
        case m if m < 5.0  => ":20:00.000"
        case m if m < 6.0  => ":25:00.000"
        case m if m < 7.0  => ":30:00.000"
        case m if m < 8.0  => ":35:00.000"
        case m if m < 9.0  => ":40:00.000"
        case m if m < 10.0 => ":45:00.000"
        case m if m < 11.0 => ":50:00.000"
        case m if m < 12.0 => ":55:00.000"
        case _             => "unkown"
   * */
  
    def getFiveDate(date: String): String = {
    val sdate = date.split(":")
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    if (sdate.length == 3) {
      val d = sdate(0).toString()
      val minute = sdate(1).toInt / 5f
      val mi = minute match {
        case m if m < 1.0  => ":00:00"
        case m if m < 2.0  => ":05:00"
        case m if m < 3.0  => ":10:00"
        case m if m < 4.0  => ":15:00"
        case m if m < 5.0  => ":20:00"
        case m if m < 6.0  => ":25:00"
        case m if m < 7.0  => ":30:00"
        case m if m < 8.0  => ":35:00"
        case m if m < 9.0  => ":40:00"
        case m if m < 10.0 => ":45:00"
        case m if m < 11.0 => ":50:00"
        case m if m < 12.0 => ":55:00"
        case _             => "unkown"
      }
//                 re=DateTime.parse(d.concat(mi),DateTimeFormat.forPattern("yyyy/MM/dd HH:mm"))
//            re.append(d).append(mi)
//      println(new Date(d.concat(mi)))
      val t = df.format(new Date(d.concat(mi)))+"Z"
//         val t = df.parse(df.format(new Date(d.concat(mi))))
         return t
    }
return   null
  }
  def main(args: Array[String]) {
    //    2017/06/05 06:28:52.000|2017/06/05 06:30:01.000|2017/06/05 06:28:52.825
    val date = "2017/06/05 06:28:52"
    //    val startdate = "2017/06/05 06:28:52.000"
    //    val enddate = "2017/06/05 06:28:52.825"
    val startdate = "2017/06/05 06:28:52.000"
    val enddate = "NULL"
    //    val sdate = date.split(":")
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(df.format(getFiveTime(date)))

    println(getTimeValue(startdate, enddate))
    println(getNowDate)
    
    val l=1504518798l
    val d ="2017-09-04 17:53:18"
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal:Calendar=Calendar.getInstance()
    cal.setTimeInMillis(1512622746801l)
    println(dateFormat.format(cal.getTime)+"==="+dateFormat.parse(d).getTime.toLong)
    println("fiveDate   :"+getFiveDate("2017/06/05 01:35:01.000"))
//    println(df.format("1503471630000"))
    //      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm")et
    //      val fdate = dateFormat.format(date)
    
//    Time: 1505728410000 ms
  }
}