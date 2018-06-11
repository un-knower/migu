package cn.ac.iie.spark.streaming.scala.kafka.Interface

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.HashMap

trait WebSiteInfoInterface {

  def getWebSiteInfo(): HashMap[String, (String, String, String, String, String, String)]

  def getWebSite(main_domain: String, webSiteBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]]): (String, String, String, String, String, String)

  def getWebChanelInfo():  HashMap[String, (String, String, String, String, String, String)]

  def getWebChanel(main_domain: String,url:String, webSiteBroadcast: Broadcast[HashMap[String, (String, String, String, String, String, String)]]): (String, String, String, String, String, String)

}
