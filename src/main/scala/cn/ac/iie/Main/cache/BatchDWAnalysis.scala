package cn.ac.iie.Main.cache

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.{Level, Logger}

import cn.ac.iie.dataImpl.cache.{BigFileAnalysis, SmallFileAnalysis}

/**
  * created by xuxf on 20180417
  * 用途：cache日志的批处理
  * 运行频率：天
  * 模型：TB_DW_Cache_small、TB_DW_Cache_big、TB_DW_Cache_big_url
  */
object BatchDWAnalysis {

  /**
    *
    * @param args
    *             args(0)=330000
    *             args(1)=small \ big \ big_url
    *             args(2)=20180418  -> 可空
    */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    var PT_CACHE_DATE = ""

    // 如果不指定批次日期，则默认运行系统日期前一天
    if(args.length == 3 && args(2).trim.length == 8){
      PT_CACHE_DATE = args(2).trim
    }else if(args.length == 2){
      val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      val cal:Calendar=Calendar.getInstance()
      cal.add(Calendar.DATE,-1)
      val yesterday=dateFormat.format(cal getTime())
      PT_CACHE_DATE=yesterday
    }else{
      System.err.println("error : args.length = " + args.length)
      System.exit(-1)
    }

    // 省份编码
    val PT_prov=args(0)

    // 大、小文件的判断参数
    val PT_param=args(1)

    if("small".equals(PT_param)){
      //对应数据模型：TB_DW_Cache_small
       SmallFileAnalysis.BatchDW_SA2DW(PT_prov, PT_CACHE_DATE)

    }else if("big".equals(PT_param)){
      //对应数据模型：TB_DW_Cache_big
       BigFileAnalysis.BatchDW_SA2DW(PT_prov, PT_CACHE_DATE)

    }else if("big_url".equals(PT_param)) {
      //对应数据模型：TB_DW_Cache_big_url
       BigFileAnalysis.BatchDW_SA2DW_Url(PT_prov, PT_CACHE_DATE)

    }else{
      System.err.println("error : args(1) = " + PT_param)
      System.exit(-1)
    }

  }

}
