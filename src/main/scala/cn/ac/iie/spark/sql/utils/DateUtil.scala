package cn.ac.iie.spark.sql.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by Administrator on 2018/4/11.
  */
object DateUtil {
  def getYesterdayTime(str:String):String={
    val arr = str.split("T")
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(arr(0))
    val time = date.getTime-86400000
    val date1 = new Date(time)
    val format01 = new SimpleDateFormat("yyyyMMdd")
    val st = format01.format(date1)
    st
  }
  def getUpdateTime(str:String):String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.MINUTE,-5)
    var current = if(str=="")dateFormat.format(cal.getTime())else dateFormat.format(str)
    current+":00"
  }
}
