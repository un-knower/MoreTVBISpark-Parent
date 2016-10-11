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
object XiaoMiLeShiMonthlyActive extends SparkSetting{

  def main(args: Array[String]) {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)

        val inputPath= s"/log/moretvloginlog/parquet/201*/loginlog"

        sqlContext.udf.register("leftsub",(x:String,y:Int) => x.substring(0,y))
        sqlContext.udf.register("getBrand",ProductModelUtils.getProductBrand _)

        sqlContext.read.load(inputPath).registerTempTable("log_data")

        sqlContext.sql("select leftsub(date,7) as ot,getBrand(productModel) as pm,count(distinct userId) from log_data " +
          "where getBrand(productModel) like '小米%' or getBrand(productModel) like '乐视%' group by " +
          "leftsub(date,7),getBrand(productModel)").show(300,false)




      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
