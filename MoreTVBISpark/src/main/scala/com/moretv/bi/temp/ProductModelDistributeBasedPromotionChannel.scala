package com.moretv.bi.temp

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.moretv.bi.util.FileUtils._


/**
 * Created by zhangyu on 2016/6/29.
  * 根据具体渠道统计新增用户中相应的设备分布,需传入一个参数：渠道名称；
  * 若不传参数，则统计所有渠道的设备分布。
  * 程序中时间区间已固定，暂不修改。
 */

object ProductModelDistributeBasedPromotionChannel extends SparkSetting {
  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)

    sqlContext.read.load("/log/dbsnapshot/parquet/20160815/" +
      "moretv_mtv_account")
      .registerTempTable("log_data")

    var sql: String = ""
    var filepath: String = ""


    if (args.nonEmpty) {
      val pc = args(0)
      sql = if(pc == "null"){
        "select product_model, mac " +
          s"from log_data where promotion_channel is null and " +
          "openTime >= '2016-07-01 00:00:00' and " +
          "openTime <= '2016-07-31 23:59:59'"
      }else {
        "select product_model, mac " +
          s"from log_data where promotion_channel = '$pc' and " +
          "openTime >= '2016-02-29 00:00:00' and " +
          "openTime <= '2016-08-13 23:59:59'"
      }
      filepath = s"/script/bi/moretv/zhangyu/file/${pc}.csv"
    } else {
      sql = "select product_model, mac " +
        s"from log_data where " +
        "openTime >= '2016-07-01 00:00:00' and " +
        "openTime <= '2016-07-31 23:59:59'"
      filepath = s"/script/bi/moretv/zhangyu/file/allPromotionChannel_201607.csv"
    }

    val rdd = sqlContext.sql(sql).
      map(row => {
        val productmodel = row.getString(0)
        val mac = row.getString(1)
        (productmodel, mac)
      })
    val res = rdd.distinct().countByKey()

    withCsvWriterOld(filepath){
      out => {
        res.foreach(x => {
          out.println(x._1 + "," + x._2)
        })
      }
    }
  }
}



