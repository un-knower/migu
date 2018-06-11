package cn.ac.iie.dataImpl

import cn.ac.iie.Interface.StreamProcessWithFlumeInterface
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.SparkFlumeEvent

object StreamProcessWithFlumeImpl extends StreamProcessWithFlumeInterface{

  override def getEventBody(): ReceiverInputDStream[SparkFlumeEvent] = ???

}
