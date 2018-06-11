package cn.ac.iie.spark.streaming.scala.kafka.Interface

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.SparkFlumeEvent

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait FlumeReceiverInterface {

  def flumeStream(flumeStream: ReceiverInputDStream[SparkFlumeEvent],spark:SparkSession,ipRulesBroadcast: Broadcast[ArrayBuffer[(String, String, String, String, String, String, String)]],ipwebBroadcast: Broadcast[mutable.HashMap[String, (String, String, String, String, String, String)]] )


}
