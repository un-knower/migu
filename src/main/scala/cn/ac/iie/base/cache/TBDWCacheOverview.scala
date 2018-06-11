package cn.ac.iie.base.cache

import java.util.Date

case class TBDWCacheOverview (
  logintime:Long,
  device_IP:String,
  method:String,
  version:String,
  host:String,
  agent:String,
  statusCode:String,
  cacheState:String,
  flow:BigInt,
  dateFormation:Date,
  cacheHitType:String,
  httpMethod:String,
  httpVersion:String,
  httpHost:String,
  code:String,
  closeStatus:String,
  flowTraffic:BigInt,
  timeValue:Long,
  responseTime:String
)extends Serializable
