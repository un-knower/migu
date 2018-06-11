package cn.ac.iie.spark.streaming.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import org.apache.spark.sql.SQLContext
import java.util.Calendar
import java.util.Date
import java.util.Properties

object NumOfConsumerAndNumOfProductorCompare {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir","D:\\ProgramFiles\\hadoop-2.6.0-cdh5.10.0")
    val sparkConf=new SparkConf().setAppName("NumOfConsumerAndNumOfProductorCompare").setMaster("local[4]").set("sp‌​ark.driver.port","180‌​80");
//    sparkConf.set("es.nodes","slave08,slave09,slave10")
//    sparkConf.set("es.port","9200")
    val (start,end)=getDays()
    val sc=new SparkContext(sparkConf)
    val sqlContext=new SQLContext(sc);
    val properties=new Properties()
    properties.put("user","root")
    properties.put("password","qwertyu")
    properties.put("driver", "com.mysql.jdbc.Driver")
    val url="jdbc:mysql://10.0.30.14:3306/rmp"
//    val predicates=Array(s"timestamp LIKE '$start%'",s"timestamp LIKE '$end%'")
//    println(predicates.toBuffer)
    val consumerDF=sqlContext.read.jdbc(url,"ip_control",properties)
//    println(consumerDF.collect().toBuffer)
//    consumerDF.orderBy("int_startip")
//    val dbUrlDF=consumerDF.drop("topic_name").drop("data_source_id").drop("crawlid").drop("response_url").drop("timestamp").drop("crawlid_stamp").drop("out_flag").drop("rid").distinct()
    consumerDF.orderBy("int_startip").rdd
//    "{  \"filter\": {    \"range\": {      \"store_time\": {        \"from\": \"2017-05-17\",        \"to\": \"2017-05-18\"      }    }  }}"
    //    val query=s"{query:{term: {operation: {value: crawl_success_url}}},filter:{range:{logTime:{from: $start,to: $end }}}}"
//    val query=s"{\"query\":{  \"filtered\":{    \"filter\":{      \"bool\":{        \"must\":[          {            \"term\": {              \"operation\": {                \"value\": \"crawl_success_url\"              }            }          },          {            \"range\":{               \"logTime\": {                \"from\": \"$start\",                \"to\": \"$end\"              }            }          }        ]      }    }  }}}"
//    val esLogDF=sqlContext.esDF("logs/SpiderLog",query)
//    val esUrlDF=esLogDF.drop("data_source_id").drop("id").drop("operation").drop("logTime").distinct()
//    esUrlDF.registerTempTable("esUrlDF")
//    dbUrlDF.registerTempTable("dbUrlDF")
//    val joinDF=sqlContext.sql(
//      """select a.esurl,b.dburl
//         from
//        (select url as esurl from esUrlDF) a
//        left join
//        (select url as dburl from dbUrlDF) b
//        on a.esurl=b.dburl
//      """)
//    //    val joinDF=esUrlDF.join(dbUrlDF,dbUrlDF("dburl")===esUrlDF("esurl"),"left_outer")
//    //    val col=new Column("esurl")
//    //    joinDF.filter("dburl is  null").drop("dburl").withColumn("day",joinDF.col("esurl"))
//    joinDF.filter("dburl is  null").rdd.saveAsTextFile("/user/yuqing/dif_detildata/result/1")
//    println(joinDF.filter("dburl is  null").count())

//    joinDF.withColumn("day",joinDF.col("esurl")).show(4)
  }
  def getDays():Tuple2[String,String]={
    val format=new SimpleDateFormat("yyyy-MM-dd")
    val now =new Date()
    val end=format.format(now)
    val cld=Calendar.getInstance()
    cld.setTime(new Date())
    cld.set(Calendar.DATE,cld.get(Calendar.DATE)-1)
    val start=format.format(cld.getTime)
    (start,end)
  }
}