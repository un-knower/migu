package cn.ac.iie.base.cache

import java.io.Serializable

case class MiguCacheOverview (
  //厂商
  factory:String,
  //省份ID
  province:String,
  //用户终端类型
  user_terminal:String,
  //域名
  domain:String,
  //业务类型
  business_type:String,
  //运营商
  operator:String,
  //请求次数
  all_num:Int,
  //请求成功次数
  req_ok_num:Int,
  //请求失败次数
  req_fail_num:Int,
  //cacheHIT次数
  req_Hit_num:Int,
  //cacheMISS次数
  req_Miss_num:Int,
  //2xx次数
  return2xx_num:Int,
  //3xx次数
  return3xx_num:Int,
  //4xx次数
  return4xx_num:Int,
  //5xx次数
  return5xx_num:Int,
  //错误代码次数
  returnerror_num:Int,
  //GET次数
  req_get_num:Int,
  //post次数
  req_post_num:Int,
  //其它请求方式次数
  req_other_num:Int,
  //流量计量单位
  flowUnit:String,
  //总流量
  all_flow:Double,
  //Hit流量
  hit_flow:Double,
  //Miss流量
  miss_flow:Double,
  //other流量
  other_flow:Double,
  //平均下载速率
  down_speed:Double,
  //回源4xx错误码次数
  back4xx_num:Int,
  //回源5xx错误码次数
  back5xx_num:Int,
  //数据更新时间
  update_datetime:String,
  //数据的业务时间频率
  business_time_type:String,
  //数据的业务时间
  business_time:String,
  //数据的业务时间小时时间戳
  business_time_1h:String,
  //数据的业务时间天时间戳
  business_time_1d:String
)extends Serializable