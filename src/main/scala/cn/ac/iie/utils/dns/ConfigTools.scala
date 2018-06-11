package cn.ac.iie.utils.dns

import java.util.Properties

object ConfigTools {

  def loadProperties(path:String) = {
    val prop = new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream(path)
    prop.load(in)
    prop
  }

}
