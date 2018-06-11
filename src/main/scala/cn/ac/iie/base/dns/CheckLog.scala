package cn.ac.iie.base.dns

case class CheckLog(
     var state:String,
     var correct_ratio:Double,
     var startTime:String,
     var endTime:String,
     var province_code:String,
     var message:String,
     var etlDate:String) {}
