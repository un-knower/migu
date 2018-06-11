package cn.ac.iie.Main.cache

import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

import cn.ac.iie.dataImpl.cache.CacheSourceTools.getFiveDate

object test {

  def main(args: Array[String]): Unit = {
    println(getBusiness_time_oot("20180605233912"))
//    println(getBusiness_time_1h(getBusiness_time("20180605233912", 11)))

//    println(new URL("http://lxqncdn.miaopai.com/stream/zQQ0ojFbJyMoQnJjbzeqDXqfne0gA8kePxHFzw___m.jpg").getHost)

  }
  def getBusiness_time(str:String,millis:Long): String ={
    var format=new SimpleDateFormat("yyyyMMddHHmmss")//指定日期格式
    val date=format.parse(str).getTime+millis
    val dateOE=new Date()
    dateOE.setTime(date)
    var dataFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm")//指定日期格式
//    println(dataFormat.format(dateOE))

    getFiveDate(dataFormat.format(dateOE))
  }

  def getBusiness_time_1h(str:String): String ={
    var format=new SimpleDateFormat("yyyy/MM/dd HH")
    val format1: String = format.format(format.parse(str))
    format1
  }

  def getBusiness_time_oot(str:String): String ={
    var trans=new SimpleDateFormat("yyyyMMddHHmmss")
    var format=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    format.format(trans.parse(str))
  }

}
