package cn.ac.iie.Main.dns

import java.net.URI

import cn.ac.iie.Service.Config
import cn.ac.iie.dataImpl.dns.{DnsOffline, ProcessDnsLog}
import cn.ac.iie.utils.dns.{DateUtil, LogUtil}

object Main {

  def main(args: Array[String]): Unit = {
    args(0) match {
      case "dns_log" =>
        val config_profile_param = Config.config_profile_param
        val hdfs_url = config_profile_param("hdfs.url")
        val hdfs_dnslog_path = config_profile_param("hdfs.dnslog.path")
        var etlDate = DateUtil.getEtlDate()
        if(args.length==4){
          etlDate = args(3)
        }
        val province_code = args(2)
        args(1) match {
          //case "online" => ProcessDnsLog.parseDnsLog(province_code)
          case "offline" => DnsOffline.parseDnsLog(province_code,etlDate)
        }
      case _ => new UnsupportedOperationException(args.mkString(","))
    }
  }


}
