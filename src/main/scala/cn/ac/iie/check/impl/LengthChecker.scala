package cn.ac.iie.check.impl

import cn.ac.iie.check.Checker
import cn.ac.iie.utils.dns.LogUtil
import org.apache.log4j.Logger
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

class LengthChecker extends Checker{

  override def check(data_source: Array[String], index: List[Int], check_param: Map[String, Any],accumulator: LongAccumulator): Boolean = {
    for (i<-index){
      val str = data_source(i)
      val length = check_param("length_limit").asInstanceOf[String].toInt
      if(str.length>length){
        //LogUtil.log(s"class:$this data_source:${data_source.mkString(",")} check ${str.length} > $length")
        accumulator.add(1)
        return false
      }
    }
    true
  }

}
