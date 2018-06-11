package cn.ac.iie.Main.dns

import cn.ac.iie.Service.Config
import cn.ac.iie.utils.dns.DateUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object DnsSample {

  def main(args: Array[String]): Unit = {
      /*val conf = Config.config_spark_param
      val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
      import spark.implicits._
      import spark.sql
      val ds = spark.read.load("/temp/ljy_test/test/DNS日志.txt")
      sql("select count(1) from remaworks_2_0_jx.TB_R_W_DNS_LOG_PARSE").show()
    //遇到了一个hdfs分区找不到的错误，原因是如果不指定分区的话，spark会去找到每一个分区，
    //如果这个分区在meatastore中存在，但是hdfs上已经不存在这个文件夹了（可能是由于人为使用某种非正常方式把分区文件夹删除了），
    //解决办法是手动从metastore中删除这条分区记录
    spark.read.textFile("hdfs://10.0.30.101:8020/temp/ljy_test/test/791_12_20171025230217a").show()*/
      var returnCodeAccumulator = mutable.HashMap[String,Int]("a"->1)
    returnCodeAccumulator("a") = 2
    returnCodeAccumulator.put("a",3)
    returnCodeAccumulator.put("b",3)
    println(returnCodeAccumulator)
  }

}
