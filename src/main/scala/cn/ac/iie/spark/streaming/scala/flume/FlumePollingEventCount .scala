package cn.ac.iie.spark.streaming.scala.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import cn.ac.iie.spark.streaming.scala.StreamingExamples
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Milliseconds
import cn.ac.iie.spark.streaming.util.IntParam

object FlumePollingEventCount {
  
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumePollingEventCount <host> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(host, IntParam(port)) = args

    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumePollingEventCount")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream that polls the Spark Sink running in a Flume agent
    val stream = FlumeUtils.createPollingStream(ssc, host, port)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events.").print()

    ssc.start()
    ssc.awaitTermination()
  }
}