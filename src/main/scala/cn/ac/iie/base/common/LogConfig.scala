package cn.ac.iie.base.common

import cn.ac.iie.check.Checker

case class LogConfig(data_limit:Int,data_separator:String,checkers:List[CheckerInfo]){

  override def toString = s"Config($data_limit, $data_separator, $checkers )"
}


case class CheckerInfo(checker:Checker, checker_param:Map[String,Any],check_index:List[Int]){

  override def toString = s"CheckerInfo($checker, $checker_param, ${check_index.mkString(",")})"
}







