package cn.ac.iie.Interface.service

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

trait StreamingContextInterface {

  def createHDFSContext(checkpointDirectory:String): StreamingContext

  def createFlumeReceiverContext(conf:SparkConf):StreamingContext

  def createSparkConf(appName:String):SparkConf



}
