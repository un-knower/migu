package cn.ac.iie.spark.sql.utils

import cn.ac.iie.spark.streaming.util.CacheSourceTools
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/4/11.
  */
object SessionUtil {
  def createSparkSession(AppName:String):SparkSession={
    val properties = CacheSourceTools.loadProperties()
    val spark=SparkSession.builder()
      .appName(AppName)
      .master("yarn")
//      .config("spark.sql.warehouse.dir","hdfs://10.0.30.101:9000/user/hive/warehouse")
      .config("spark.sql.warehouse.dir",properties.getProperty("spark.sql.warehouse.dir"))
      .config("es.index.auto.create", "true")
      .config("es.nodes", properties.getProperty("es.nodes"))
      .config("es.port", properties.getProperty("es.port"))
//      .config("spark.executor.instances","4")
//      .config("spark.cores.max", "12")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }
}
