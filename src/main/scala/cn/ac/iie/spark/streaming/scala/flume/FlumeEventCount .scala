package cn.ac.iie.spark.streaming.scala.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel
import cn.ac.iie.spark.streaming.scala.StreamingExamples
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Milliseconds
import cn.ac.iie.spark.streaming.util.IntParam

object FlumeEventCount  {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumeEventCount <host> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(host, IntParam(port)) = args

    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventCount")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    ssc.start()
    ssc.awaitTermination()
}
}