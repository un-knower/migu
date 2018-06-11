package cn.ac.iie.spark.streaming.scala.kafka.Impl

import cn.ac.iie.spark.streaming.scala.kafka.Interface.SaveDataInterface
import cn.ac.iie.spark.streaming.util.CacheSourceTools
import org.apache.spark.sql.{DataFrame, Dataset}


object SavaDataImpl extends SaveDataInterface{

  override def SaveDataToHDFS(dataFrame: DataFrame,path:String): Unit = {

    dataFrame.repartition(3).write.orc(path)
//    println("写入数据到路径:"+path+"HDFS写入成功"+CacheSourceTools.getNowDateJustmm+"共:"+dataFrame.count()+"条")
  }

  override def SaveDataToHDFS(df: Dataset[Any]): Unit = {

  }

  override def SaveDataToES(df: DataFrame,path:String): Unit = {


  }
}
