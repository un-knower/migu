package cn.ac.iie.check.impl

import java.lang.reflect.Method

import cn.ac.iie.check.Checker
import cn.ac.iie.utils.dns.LogUtil
import org.apache.spark.util.LongAccumulator

class FunctionChecker extends Checker{

  var method:Method = null

  override def check(data_source: Array[String], index: List[Int], check_param: Map[String, Any], accumulator: LongAccumulator): Boolean = {
    val function = check_param("function").asInstanceOf[String]
    val sp = function.split("#")
    val clazz = Class.forName(sp(0))
    method = clazz.getDeclaredMethod("isValidDomainForDpi")
    val value = method.invoke(clazz,"aa")
    println(value)
   /* for (i<-index){
      val str = data_source(i)
      val trim = str.trim

    }*/
    true
  }


}

object FunctionChecker{

  def main(args: Array[String]): Unit = {
    val clazz = Class.forName("cn.ac.iie.dataImpl.dpi.ProcessDpiLog")
    val method = clazz.getDeclaredMethod("isValidDomainForDpi",Class.forName("java.lang.String"))
    val value = method.invoke(clazz,"www.baidu.com")
    println(value)
  }

}
