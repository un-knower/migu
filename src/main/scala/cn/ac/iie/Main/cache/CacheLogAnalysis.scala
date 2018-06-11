package cn.ac.iie.Main.cache

import cn.ac.iie.dataImpl.cache.{BigFileAnalysis, SmallFileAnalysis}

object CacheLogAnalysis {

    def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

      if(args.length!=2){
        println("请输入参数")
        System.exit(0)
      }
      if(args(0).equals("small")){
          SmallFileAnalysis.OverrideToEsByFlume(args(1))
      }else if(args(0).equals("big")){
          BigFileAnalysis.OverrideToEsByFlume(args(1))
      }


//      SmallFileAnalysis.OverrideToEsByFlume()


  }



}
