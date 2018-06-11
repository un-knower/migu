package cn.ac.iie.Interface

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.SparkFlumeEvent

trait StreamProcessWithFlumeInterface {

  def getEventBody():ReceiverInputDStream[SparkFlumeEvent]

}
