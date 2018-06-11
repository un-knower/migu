package cn.ac.iie.base.dns

class DnsLog(val source_ip:String,val domain:String,val time:String,val target_ip:String,val return_code:String) {


}

object DnsLog{
  def apply(arr:Array[String])={
    new DnsLog(arr(0),arr(1),arr(2),arr(3),arr(4))
  }
}
