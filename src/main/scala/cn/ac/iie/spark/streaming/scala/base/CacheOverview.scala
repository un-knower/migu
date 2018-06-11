package cn.ac.iie.spark.streaming.scala.base

/**
  * 缓存日志
  */
case class CacheOverview(
    //单位时间段内每一条数据的唯一标识列
    rowid:Int,
    //厂商
    factory:String,
    //缓存类型
    cache_type:String,
    //缓存设备信息
    cache_eqp_info:String,
    //地市
    city:String,
    //接入方式
    access_type:String,
    //用户终端类型
    user_terminal:String,
    //网站ID
    webSiteID:String,
    //网站名
    webSiteName:String,
    //域名
    domain:String,
    //文件类型
    file_type:String,
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
    //cacheDenied次数
    req_Denied_num:Int,
    //cacheError次数
    req_Error_num:Int,
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
    //DENIED流量
    denied_flow:Double,
    //Miss流量
    miss_flow:Double,
    //缓存Miss 流量
    cacheMiss_flow:Double,
    //平均下载速率
    down_speed:Double,
    //回源4xx错误码次数
    back4xx_num:Int,
    //回源5xx错误码次数
    back5xx_num:Int,
    //首包时延
    fstpack_delay:Int,
    //回源首包时延
    back_fstpack_delay:Int,
    //DNS解析失败次数
    dnsparse_fail_num:Int,
    //回源建链失败次数
    backlink_fail_num:Int,
    //不应缓存流量
    nocache_flow:Double,
    //请求增益比
    //req_gainratio:Double,
    //流量增益比
    //flow_gainratio:Double,
    //请求miss率
    //req_missratio:Double,
    //回源流量
    back_flow:Double,
    //源站(回源)下载速率
    back_downspeed:Double,
    //数据更新时间
    update_datetime:String,
    //数据的业务时间频率
    business_time_type:String,
    //数据的业务时间
    business_time:String,
    //数据的业务时间频率
    ds_type:String,
    //数据的业务时间
    ds:String,
    //小时时间戳
    business_time_1h:String,
    //天时间戳
    business_time_1d:String

)extends Serializable