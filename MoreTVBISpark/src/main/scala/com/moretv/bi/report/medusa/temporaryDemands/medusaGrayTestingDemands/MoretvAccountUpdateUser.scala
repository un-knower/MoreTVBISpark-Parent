package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object MoretvAccountUpdateUser extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "5g").
          set("spark.executor.cores", "5").
          set("spark.cores.max", "200")
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val startDate = p.startDate
        val outputPath=s"/log/medusa/updateInfo/$startDate/moretvAccountUpdateUser"


          val medusaRdd = sqlContext.read.parquet("/log/medusa/parquet/2016{06*,052*,053*}/*").select("userId").map(r=>r
          .getString(0)).distinct()
        val moretvAccountRdd = sqlContext.read.parquet("/mbi/parquet/mtvaccount/20160{2*,3*,4*,5*,6*}").select("userId")
          .map(r=>r.getString(0)).distinct()

          val moretvAccountUpdateUser=medusaRdd.intersection(moretvAccountRdd)
          val df = moretvAccountUpdateUser.toDF("userId")
          df.write.parquet(outputPath)


      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
