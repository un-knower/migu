package cn.ac.iie.spark.streaming.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkContextSingleton {
   @transient private var instance: SparkContext = _

  def getInstance(sparkConf: SparkConf): SparkContext = {
    if (instance == null) {
      instance = SparkContext.getOrCreate(sparkConf)

    }
    instance
  }
}