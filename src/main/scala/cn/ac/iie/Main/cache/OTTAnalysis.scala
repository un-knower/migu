package cn.ac.iie.Main.cache

import cn.ac.iie.Service.Config
import cn.ac.iie.base.cache.MiguCacheOverview
import cn.ac.iie.base.common.IpControl
import cn.ac.iie.dataImpl.cache.{CacheSourceTools, SparkSessionSingleton}
import cn.ac.iie.utils.dns.{DateUtil, JdbcUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

object OTTAnalysis {

  def main(args: Array[String]): Unit = {
      var readPath="C:\\Users\\tan\\Desktop\\ott2"
      var factory="yunfan"
      val conf = Config.config_spark_param
      var etldate = DateUtil.getEtlDate()
      if(args.length==3){
        readPath = args(0)
        factory = args(1)
        etldate = args(2)
      }
      val session: SparkSession = SparkSessionSingleton.getInstance(conf)

      val textFile = session.read.textFile(readPath)

      val ipRulesBroadcast: Broadcast[Array[IpControl]] = session.sparkContext.broadcast(JdbcUtil.getIpControl)
    import session.implicits._
    val dataSource = textFile.map(line => {
        line.split("\\|")
      }).filter(line => line.length >= 14)
    val MiguDs = dataSource.map(
    line => {
          val result:MiguCacheOverview=CacheSourceTools.getOTTAnalysis(line,factory, ipRulesBroadcast)
          result
        })
      //原始数据汇总五分钟
      MiguDs.createOrReplaceTempView("MiguBaseDF")
      //val result_day_5m=session.sql("select '"+factory+"' as factory,province,user_terminal,domain,business_type,operator,count(1) as all_num,sum(req_ok_num) as req_ok_num,sum(req_fail_num) as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,flowUnit,sum(all_flow) as all_flow,sum(hit_flow) as hit_flow,sum(miss_flow) as miss_flow,sum(other_flow) as other_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,update_datetime,business_time_type,business_time,business_time_1h,business_time_1d from MiguBaseDF group by province,user_terminal,business_type,operator,flowUnit,update_datetime,business_time_type,business_time,business_time_1h,business_time_1d,domain")
      val result_day_5m=session.sql(
        s"""
          SELECT
            '$factory' AS factory,
            province,
            user_terminal,
            domain,
            business_type,
            operator,
            COUNT(1) AS all_num,
            SUM(req_ok_num) AS req_ok_num,
            SUM(req_fail_num) AS req_fail_num,
            SUM(req_Hit_num) AS req_Hit_num,
            SUM(req_Miss_num) AS req_Miss_num,
            SUM(return2xx_num) AS return2xx_num,
            SUM(return3xx_num) AS return3xx_num,
            SUM(return4xx_num) AS return4xx_num,
            SUM(return5xx_num) AS return5xx_num,
            SUM(returnerror_num) AS returnerror_num,
            SUM(req_get_num) AS req_get_num,
            SUM(req_post_num) AS req_post_num,
            SUM(req_other_num) AS req_other_num,
            flowUnit,
            SUM(all_flow) AS all_flow,
            SUM(hit_flow) AS hit_flow,
            SUM(miss_flow) AS miss_flow,
            SUM(other_flow) AS other_flow,
            AVG(down_speed) AS down_speed,
            SUM(back4xx_num) AS back4xx_num,
            SUM(back5xx_num) AS back5xx_num,
            update_datetime,
            business_time_type,
            business_time,
            business_time_1h,
            business_time_1d
          FROM MiguBaseDF
          GROUP BY
            province,
            user_terminal,
            business_type,
            operator,
            flowUnit,
            update_datetime,
            business_time_type,
            business_time,
            business_time_1h,
            business_time_1d,
            domain
        """)

    //从五分钟汇总到小时
      result_day_5m.createOrReplaceTempView("result_day_5m")
      val result_day_1h=session.sql("select factory,province,user_terminal,domain,business_type,operator,count(1) as all_num,sum(req_ok_num) as req_ok_num,sum(req_fail_num) as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,flowUnit,sum(all_flow) as all_flow,sum(hit_flow) as hit_flow,sum(miss_flow) as miss_flow,sum(other_flow) as other_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,update_datetime,'1h' as business_time_type,business_time_1h,business_time_1d from result_day_5m group by factory,province,user_terminal,business_type,operator,flowUnit,update_datetime,business_time_type,business_time_1h,business_time_1d,domain")

      //从小时汇总到天
      result_day_1h.createOrReplaceTempView("result_day_1h")
      val result_day_1d=session.sql("select factory,province,user_terminal,domain,business_type,operator,count(1) as all_num,sum(req_ok_num) as req_ok_num,sum(req_fail_num) as req_fail_num,sum(req_Hit_num) as req_Hit_num,sum(req_Miss_num) as req_Miss_num,sum(return2xx_num) as return2xx_num,sum(return3xx_num) as return3xx_num,sum(return4xx_num) as return4xx_num,sum(return5xx_num) as return5xx_num,sum(returnerror_num) as returnerror_num,sum(req_get_num) as req_get_num,sum(req_post_num) as req_post_num,sum(req_other_num) as req_other_num,flowUnit,sum(all_flow) as all_flow,sum(hit_flow) as hit_flow,sum(miss_flow) as miss_flow,sum(other_flow) as other_flow,avg(down_speed) as down_speed,sum(back4xx_num) as back4xx_num,sum(back5xx_num) as back5xx_num,update_datetime,'1d' as business_time_type,business_time_1d from result_day_1h group by factory,province,user_terminal,business_type,operator,flowUnit,update_datetime,business_time_type,business_time_1d,domain")


      EsSparkSQL.saveToEs(result_day_5m, s"analysis_cdn_$etldate/CDNAnalysis_5m")
      EsSparkSQL.saveToEs(result_day_1h, s"analysis_cdn_$etldate/CDNAnalysis_1h")
      EsSparkSQL.saveToEs(result_day_1d, s"analysis_cdn_$etldate/CDNAnalysis_1d")

    }
//http://58.244.48.32:9200/


}
