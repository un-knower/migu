package cn.ac.iie.spark.streaming.scala.kafka.Main

import cn.ac.iie.spark.streaming.scala.kafka.Service.StreamingContextService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object ReadDataOnHive {

  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println("请输入参数")
    }
    val conf= StreamingContextService.createSparkConf("OverviewToESByFlume_Small").set("hive.metastore.warehouse.dir","hdfs://10.0.30.101:8020/user/hive/warehouse")
.set("spark.sql.warehouse.dir","hdfs://10.0.30.101:8020/user/hive/warehouse")
    println(conf.get("hive.metastore.warehouse.dir"))

    val sc=new SparkContext(conf)

    val hiveContext=new HiveContext(sc)
    hiveContext.sql("use ifp_source").show()
    hiveContext.sql("show tables")



  }

}
