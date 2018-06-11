package cn.ac.iie.spark.streaming.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.sql.Date
case class DnsLog(ip_source: String, domain: String, dd_time_collect: String, ip_target: String, dns_parse_type: Int)

object DnsConvert {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConvertDns").setMaster("spark://10.0.30.101:7077")
//        val conf = new SparkConf().setAppName("ConvertDns").setMaster("local[10]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("user.name", "hadoop-user")
    conf.set("spark.sql.parquet.compression.codec", "gzip")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._
    //读取数据
    //    val dns = spark.read.textFile("hdfs://10.0.30.101:8020/user/hadoop-user/coll_data/dns_log/jiangxi/791/20171025")
    val files: RDD[String] = sc.textFile("hdfs://10.0.30.101:8020/user/hadoop-user/coll_data/dns_log/jiangxi/791/20171026")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateFormat_ = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //2017/08/04 23:55:01
    //将String=》Row
    val DnsData = files.map(_.split("\\|")).filter(file=>file(2).length()==14).map(file => DnsLog(file(0),
      file(1),
      dateFormat_.format(new Date(dateFormat.parse(file(2)).getTime)),
      file(3),
      file(4).toInt))
    val DnsdataFrame = DnsData.toDF()
//        DnsdataFrame.show()
    DnsdataFrame.write.parquet("hdfs://10.0.30.101:8020/ifp_source/DataCenter/sa/TB_R_W_DNS_LOG/20171026/2017120415/data/store")

    sc.stop()

  }
}