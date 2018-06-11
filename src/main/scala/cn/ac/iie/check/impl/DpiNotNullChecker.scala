package cn.ac.iie.check.impl

import cn.ac.iie.check.Checker
import cn.ac.iie.utils.dns.LogUtil
import org.apache.spark.util.LongAccumulator

class DpiNotNullChecker extends Checker{

  override def check(data_source: Array[String], index: List[Int], check_param: Map[String, Any], accumulator: LongAccumulator): Boolean = {
    for (i<-index){
      val str = data_source(i)
      val trim = str.trim
      if(str==null||str.trim.==("")||str.startsWith("F")){
        LogUtil.log(s"class:$this data_source:${data_source.mkString(",")} check[$i] ${data_source(i)}, is null")
        accumulator.add(1)
        return false
      }
    }
    true
  }
}
