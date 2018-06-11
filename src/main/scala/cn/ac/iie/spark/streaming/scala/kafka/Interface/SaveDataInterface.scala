package cn.ac.iie.spark.streaming.scala.kafka.Interface

import org.apache.spark.sql.{DataFrame, Dataset}

trait SaveDataInterface {

  def SaveDataToHDFS(df:DataFrame,path:String)

  def SaveDataToHDFS(df:Dataset[Any])

  def SaveDataToES(df:DataFrame,path:String)


}
