package cn.ac.iie.utils.dns

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit, TemporalAccessor}
import java.time._
import java.util
import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object DateUtil {

  val dateFormatter = DateTimeFormatter.BASIC_ISO_DATE
  val dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
  val dateTimeFormatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def getCurrentDate(dtf:DateTimeFormatter=dateFormatter)={
    LocalDate.now().format(dateFormatter)
  }

  def getCurrentDateTime(dtf:DateTimeFormatter=dateTimeFormatter)={
    val date = LocalDateTime.now()
    //val str = date.format(dateTimeFormatter)
    val str = dateTimeFormatter2.format(date)
    str
  }

  //获取昨天日期
  def getEtlDate(dtf:DateTimeFormatter=dateFormatter) ={
    val etlDate = LocalDate.now().minusDays(1)
    etlDate.format(dtf)
  }

  def getCustomDate(pattern:String)(instant: Instant): String ={
    val datetime = LocalDateTime.ofInstant(instant,ZoneId.of("Asia/Shanghai"))
    val formatter = DateTimeFormatter.ofPattern(pattern)
    formatter.format(datetime)
  }

  def genDpiSeqTimeSource(startTime:String,endTime:String): List[String] ={
    var list = ListBuffer[String]()
    val formatter = DateTimeFormatter.ofPattern("HHmm")
    var time = LocalTime.parse(startTime,formatter)
    var time2 = LocalTime.parse(endTime,formatter)
    val m1 = time.getMinute%5
    val m2 = time2.getMinute%5
    if(m1!=0){
      time = time.plusMinutes(5-m1)
    }
    if(m2!=0){
      time2 = time2.plusMinutes(5-m2)
    }
    val l = ChronoUnit.MINUTES.between(time, time2)
    if(l>=0){
      val num = l/5
      for(i<-0 to num.toInt){
        val str = formatter.format(time2)
        list+:=str
        time2 = time2.minusMinutes(5)
      }
    }else{
      val mins = time2.getMinute/5+time2.getHour*12
      for(i<-0 to mins.toInt){
        val str = formatter.format(time2)
        list+:=str
        time2 = time2.minusMinutes(5)
      }
    }
    list.toList
  }

  def main(args: Array[String]): Unit = {
    //new
    println(genDpiSeqTimeSource("2323","0052").mkString(","))
    /*val startInstant = Instant.ofEpochMilli("1505643992985".substring(0,13).toLong)
    println(getCustomDate("yyyy-MM-dd HH:mm:ss")(startInstant))*/
  }

}
