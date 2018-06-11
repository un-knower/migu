package cn.ac.iie.check.impl

import cn.ac.iie.check.Checker
import cn.ac.iie.utils.dns.LogUtil
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.util.matching.Regex

class RegexChecker extends Checker{

  var regex_memory = new mutable.HashMap[String,Regex]()

  override def check(data_source: Array[String], index: List[Int], checker_param: Map[String, Any],accumulator: LongAccumulator): Boolean = {
    for (i<-index){
      val regex = checker_param("regex").asInstanceOf[String]
      var reg:Regex = regex_memory.getOrElse(regex,null)
      if(reg==null){
        reg = regex.r
        regex_memory+=(regex->reg)
      }
      val matches = reg.findAllMatchIn(data_source(i))
      if(matches.isEmpty){
        //LogUtil.log(s"class:$this data_source:${data_source.mkString(",")} check ${data_source(i)}, $reg false")
        accumulator.add(1)
        return false
      }
    }
    true
  }
}
