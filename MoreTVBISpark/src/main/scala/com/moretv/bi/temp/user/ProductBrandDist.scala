package com.moretv.bi.temp.user

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

/**
  * 创建人黄虎
  * 创建时间 2016:07:06
  * 程序作用：统计用户新增活跃终端分布
  */
object ProductBrandDist extends SparkSetting{

  def main(args: Array[String]) {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)

        val inputPathNew= s"/log/dbsnapshot/parquet/20160805/moretv_mtv_account"
        sqlContext.udf.register("getBrand",ProductModelUtils.getProductBrand _)

        sqlContext.read.load(inputPathNew).registerTempTable("log_data")

        sqlContext.sql("select getBrand(product_model),count(distinct mac) from log_data " +
          "where openTime <= '2016-08-05 23:59:59' group by getBrand(product_model)").
          collectAsList().foreach(println)
        sqlContext.sql("select count(distinct mac) from log_data where openTime <= '2016-08-05 23:59:59' " +
          "and userType <> 1").show()

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
