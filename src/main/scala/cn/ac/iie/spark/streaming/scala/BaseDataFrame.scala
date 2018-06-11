package cn.ac.iie.spark.streaming.scala

import org.apache.spark.SparkConf

/**
 * Created by dell on 2016/8/1.
 */
trait BaseDataFrame {

  private [iie] val sparkConf = new SparkConf()
  sparkConf.setAppName("Hours")
  sparkConf.setMaster("local[2]")

}
