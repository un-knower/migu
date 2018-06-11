package cn.ac.iie.base.dpi

class DpiLog(
            val host:String,
            val uri:String,
            val _interface:String, //接口类型，16进制编码
            val procedure_start_time:String,//TCP/UDP流开始时间，UTC时间），从1970/1/1 00:00:00开始到当前的微秒数。
            val procedure_end_time:String,//TCP/UDP流结束时间，UTC时间），从1970/1/1 00:00:00开始到当前的微秒数。
            val app_type_code:String,//业务类型编码
            val protocol_type:String,//协议类型
            val user_IPv4:String,//终端用户的IPv4地址
            val app_server_IP_IPv4:String,//访问服务器的IPv4地址
            val app_server_port:String,//访问的服务器的端口
            val ul_data:String,//上行流量,单位：字节
            val dl_data:String,//下行流量,单位：字节
            val tcp_response_time:String,//TCP建链响应时延（微秒）
            val tcp_ack_time:String,//TCP建链确认时延（微秒）
            val first_req_time:String,//TCP建链成功到第一条事务请求的时延（微秒）
            val first_response_time:String,//第一条事务请求到其第一个响应包时延（微秒）
            val first_http_response_time:String,//第一个HTTP响应包相对于第一个HTTP请求包（如get命令）的时延，单位微秒
            val http_version:String,
            val message_status:String,//HTTP/WAP2.0层的响应码
            val user_agent:String,//终端向访问网站提供的终端信息，包括IMEI、浏览器类型等
            val http_content_type:String,//HTTP的内容是文字还是图片、视频、应用等
            val refer_URI:String,//参考URI
            val cookie:String,
            val content_length:String,//协议中Content-Length字段
            val IE:String//浏览工具*/
            )extends Product {

  override def toString = s"DpiLog(host=$host, uri=$uri, interface=${_interface}, procedure_start_time=$procedure_start_time, procedure_end_time=$procedure_end_time, app_type_code=$app_type_code, protocol_type=$protocol_type, user_IPv4=$user_IPv4, app_server_IP_IPv4=$app_server_IP_IPv4, app_server_port=$app_server_port, ul_data=$ul_data, dL_data=$dl_data, tcp_response_time=$tcp_response_time, tcp_ack_time=$tcp_ack_time, first_req_time=$first_req_time, first_response_time=$first_response_time, first_http_response_time=$first_http_response_time, http_version=$http_version, message_status=$message_status, user_agent=$user_agent, http_content_type=$http_content_type, refer_URI=$refer_URI, cookie=$cookie, content_length=$content_length, IE=$IE)"

  override def productElement(n: Int): Any = ???

  override def productArity: Int = ???

  override def canEqual(that: Any): Boolean = {
    that match {
      case t:DpiLog => true
      case _ => false
    }
  }

}

object DpiLog{

  def apply(arr:Array[String]) = {
    new DpiLog(arr(70),arr(72),arr(2),arr(5),arr(6),arr(4),arr(9),arr(15),arr(19),arr(21),arr(22),arr(23),
      arr(32),arr(33),arr(36),arr(37),arr(67),arr(64),arr(66),arr(75),arr(76),arr(78),arr(80),arr(81),arr(86))
  }

}