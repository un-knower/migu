package cn.ac.iie.spark.streaming.scala.base

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import cn.ac.iie.spark.streaming.util.IpTools
import cn.ac.iie.spark.streaming.util.WebSiteInfo
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

trait BaseDataFrame {

  private[spark] val sparkConf = new SparkConf()
  //==================================================
  sparkConf.setAppName("DataProcess")
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.set("spark.shuffle.consolidateFiles", "true")
  sparkConf.set("park.speculation", "true")
  //    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "50")
  sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
  sparkConf.setExecutorEnv("SPARK_DAEMON_JAVA_OPTS", " -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
  sparkConf.registerKryoClasses(Array(classOf[cn.ac.iie.spark.streaming.scala.base.CacheAccessBean], classOf[cn.ac.iie.spark.streaming.java.GetDomainInfo], classOf[cn.ac.iie.spark.streaming.java.GetFileInfo]))

  sparkConf.set("num-executors", "8")
  sparkConf.set("executor-cores", "2")
  sparkConf.set("executor-memory", "30G")
  sparkConf.set("driver-memory", "10G")
  //==================================================
//      sparkConf.set("spark.executor.memory", "4G")
//      sparkConf.set("spark.executor.cores", "2")
//      sparkConf.set("spark.driver.memory", "2G")

  //    sparkConf.set("spark.executor.memory", "30G")
  //    sparkConf.set("spark.executor.cores", "2")
  //    sparkConf.set("spark.driver.memory", "10G")
//           sparkConf.setMaster("yarn")
//  sparkConf.setMaster("local[2]")
//  sparkConf.setMaster("spark://10.0.30.101:7077")
  sparkConf.set("es.index.auto.create", "true")
  sparkConf.set("es.nodes", "10.0.30.101,10.0.30.102,10.0.30.105,10.0.30.107")
  sparkConf.set("es.port", "9200")
  //  sparkConf.set("es.mapping.date.rich", "false")
  //  sparkConf.set("date_detection", "false")
  //    sparkConf.set("spark.sql.shuffle.partitions", "20")
  sparkConf.set("pushdown", "true")
  //==================================================
 /* private[spark] val ssc = new StreamingContext(sparkConf, Seconds(120))
  private[spark] val sparkContext = ssc.sparkContext
  //  sparkContext.setLogLevel("DEBUG")
  private val topics = "cachetest";
  private[spark] val topicsSet = topics.split(",").toSet
  private val brokers = "node03:9092,node04:9092"
  private[spark] val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
*/
}