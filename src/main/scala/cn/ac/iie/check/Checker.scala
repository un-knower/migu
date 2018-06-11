package cn.ac.iie.check

import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

trait Checker extends Serializable{

  def check(data_source:Array[String],index:List[Int],check_param:Map[String,Any],accumulator:LongAccumulator):Boolean

}
