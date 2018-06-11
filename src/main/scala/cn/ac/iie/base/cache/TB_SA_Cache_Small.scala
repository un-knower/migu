package cn.ac.iie.base.cache

case class TB_SA_Cache_Small(
  //日志记录时间
  date: String,
  //缓存设备信息
  deviceip: String,
  //用户IP地址
  userip: String,
  //用户请求IP地址
  serverip: String,
  //HTTP Method
  httpmethod: String,
  //回源HTTP Version
  httpversion: String,
  //回源HTTP Host
  host: String,
  //回源HTTP Uri
  uri: String,
  //HTTP User-agent
  useragent: String,
  //HTTP Referer
  referer: String,
  //HTTP Content-type
  contenttype: String,
  //HTTP Status-code
  statuscode: String,
  //缓存命中状态
  cachestate: String,
  //缓存服务端口
  cachehost: String,
  //缓存吐出流量
  cacheflow: String,
  //请求开始时间
  requeststartdate: String,
  //请求结束时间
  requestenddate: String,
  //首字节响应时间
  firstresponsetime: String,
  //URL Hash值
  urlhashcode: String,
  //缓存命中类型
  cachetype: String,
  //回源源IP地址
  backsourceip: String,
  //回源目的IP地址
  backtargetip: String,
  //回源HTTP Method
  backhttpmethod: String,
  //回源HTTP Version
  backhttpversion: String,
  //回源HTTP Host
  backhost: String,
  //回源HTTP Uri
  backuri: String,
  //回源HTTP Content-type
  backcontenttype: String,
  //回源HTTP Status-code
  backstate: String,
  //回源关闭状态
  backclosestate: String,
  //回源Cache-Control
  backcachecontrol: String,
  //回源Max-age
  backmaxgge: String,
  //回源文件大小
  backfilesize: String,
  //缓存保存状态
  backsavestate: String,
  //回源服务端口
  backserverport: String,
  //回源流量
  backflow: String,
  //回源请求开始时间
  backrequeststartdate: String,
  //回源请求结束时间
  backrequestenddate: String,
  //源站首字节响应时间
  backfirstresponsetime: String,
  //厂商
  factory: String,
  //缓存类型
  cache_type: String,
  //地市
  city: String,
  //接入方式
  access_type: String,
  //用户终端类型
  user_terminal: String,
  //网站ID
  websiteid: String,
  //网站名（中文）
  websitename: String,
  //泛域名
  main_domain: String,
  //文件类型
  file_type: String

) extends Serializable
