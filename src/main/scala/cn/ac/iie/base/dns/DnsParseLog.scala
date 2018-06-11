package cn.ac.iie.base.dns

case class DnsParseLog(
var biz_time:String,
var domain:String,
var fan_domain:String,
var level_domain:String,
var target_ip:String ,
var oper_code:String,
var target_province_code:String,
var sys_code:String,
var user_type:String,
var province_code:String,
var return_code:String) {}
