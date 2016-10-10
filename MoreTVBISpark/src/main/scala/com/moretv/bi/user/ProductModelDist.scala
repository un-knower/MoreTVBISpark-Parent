package com.moretv.bi.user

import com.moretv.bi.util.ParamsParseUtil._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DateFormatUtils, ProductModelUtil, SparkSetting}
import org.apache.spark.SparkContext
import com.moretv.bi.util.FileUtils._
import java.util.Date

import scala.collection.JavaConversions._
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/8/5.
 */
object ProductModelDist extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(ProductModelDist,args)
  }
  override def execute(args: Array[String]) {
    withParse(args) {
      p => {
//        sqlContext.udf.register("getProductBrand",ProductModelUtil.getProductBrand _)
        val whichMonth = p.whichMonth
        val inputDate = p.startDate
        sqlContext.read.load(s"/log/moretvloginlog/parquet/$whichMonth*/loginlog").registerTempTable("active_log")
        sqlContext.read.load(s"/log/dbsnapshot/parquet/$inputDate/moretv_mtv_account").registerTempTable("total_log")

        val day = DateFormatUtils.toDateCN(inputDate,0)
        val activeDist = sqlContext.sql("select productModel,count(distinct userId) " +
          "from active_log group by productModel").collectAsList()
        val totalDist = sqlContext.sql("select product_model,count(distinct user_id) " +
          s"from total_log where openTime <= '$day 23:59:59' group by product_model").collectAsList()

        withCsvWriterOld(s"/script/bi/moretv/liankai/file/productModelBrandActiveDist_$whichMonth.csv"){
          out => {
            out.println(new Date())
            activeDist.foreach(row => {
              out.println(row.get(0)+","+row.get(1))
            })
          }
        }

        withCsvWriterOld(s"/script/bi/moretv/liankai/file/productModelBrandTotalDist_$whichMonth.csv"){
          out => {
            out.println(new Date())
            totalDist.foreach(row => {
              out.println(row.get(0)+","+row.get(1))
            })
          }
        }
      }
    }

  }


}
