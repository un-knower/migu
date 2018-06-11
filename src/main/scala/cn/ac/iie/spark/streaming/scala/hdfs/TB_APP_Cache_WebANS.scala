package cn.ac.iie.spark.streaming.scala.hdfs

import cn.ac.iie.spark.streaming.util.{CacheSourceTools, SparkSessionSingleton}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import cn.ac.iie.spark.streaming.scala.kafka.Overview_Day_ToES

object TB_APP_Cache_WebANS {
  def main(args: Array[String]): Unit = {
    val properties = CacheSourceTools.loadProperties()
    @transient
    val sparkConf = new SparkConf().setAppName("TB_APP_Cache_WebANS").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("es.index.auto.create", "true")
    sparkConf.set("user.name",properties.getProperty("user.name"))
//        sparkConf.setMaster("spark://10.0.30.101:7077")
    sparkConf.setMaster("local[2]")
    sparkConf.set("es.nodes",properties.getProperty("es.nodes"))
    sparkConf.set("es.port", properties.getProperty("es.port"))
    //    var dataInputPath="hdfs://10.0.30.101:8020/user/hadoop-user/coll_data/cache_log/jiangxi/791/20171025/FlumeData.1512463927350"
    val context: SparkContext = new SparkContext(sparkConf)
    val sql = new SQLContext(context)
    val timeSplit= CacheSourceTools.getFiveTime(CacheSourceTools.getNowDateIncloudmm()).split(" ")
    val time=Overview_Day_ToES.getTime(timeSplit(0))
    println("日期："+time._1+"读取"+time._2+"的数据")
    val load = sql.read.format("parquet").load("G:\\atest\\ifp_source\\DataCenter\\sa\\TB_SA_CACHE\\330000\\20171220\\small\\201712201700\\data\\correct_store")
//    val load = sql.read.format("parquet").load("G:\\atest\\ifp_source\\DataCenter\\sa\\TB_SA_CACHE\\330000\\20171220\\small\\201712201700\\data\\correct_store")
//    val load = sql.read.format("parquet").load("hdfs://10.0.30.101:8020/ifp_source/DataCenter/SA/TB_SA_Cache_small/"+time._2+"/330000/")
    //    val textFile: RDD[String] = context.textFile(dataInputPath)
    //    @transient
    val spark = SparkSessionSingleton.getInstance(sparkConf)
    import spark.implicits._
    load.createOrReplaceTempView("WebANS")
    CacheSourceTools.getNowDateIncloudmm()
    //    val df = sql.sql("select factory,cache_type,access_type,user_terminal,websiteid,websitename,main_domain as domain,file_type,count(*) as all_num from WebANS")
//    val df = sql.sql("select  DATE_FORMAT( '2012-01-12', '%Y-%m-%d') as a from  WebANS ")
//    val df = sql.sql("select (select convert(varchar(100), requestenddate, 112)) as da from WebANS")

    val df:DataFrame = sql.sql("select " +
      "userip,count(1) as allnum "+
      ",'1' as rowid,'huawei' as factory " +
      ",cache_type,access_type" +
      ",user_terminal " +
      ",websiteid,websitename,main_domain as domain " +
      ",'"+CacheSourceTools.getNowDateIncloudmm()+"' as update_datetime "+
      ",file_type" +
      " from WebANS  group by userip,cache_type,main_domain,file_type,access_type,user_terminal,websiteid,websitename,user_terminal,websiteid,websitename ")
//    df.show()  //requestenddate
df.write.parquet("")


  }


}
