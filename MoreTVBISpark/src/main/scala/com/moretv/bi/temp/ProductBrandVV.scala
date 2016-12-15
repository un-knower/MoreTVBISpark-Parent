package main.scala.com.moretv.bi.temp


import com.moretv.bi.util.ParamsParseUtil._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ProductModelUtil, ProductModelUtils, SparkSetting}
import org.apache.spark.SparkContext
import com.moretv.bi.util.FileUtils._
import java.util.Date

import org.apache.spark.storage.StorageLevel
import scala.collection.JavaConversions._
import org.apache.spark.sql.SQLContext

/**
  * Created by czw on 2016/9/4.
  */
object ProductBrandVV extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(ProductBrandVV, args)
  }

  override def execute(args: Array[String]) {
    withParse(args) {
      p => {

        sqlContext.udf.register("getProductBrand", ProductModelUtils.getProductBrand _)

        val path1 = "/log/moretvloginlog/parquet/201610*/loginlog"
        sqlContext.read.parquet(path1)
          .filter("day between '2016-10-01' and '2016-10-31'")
          .select("productModel", "mac")
          .registerTempTable("log_data")

        val monthproducthuv = sqlContext.sql(
          """
            |select getProductBrand(productModel), count(distinct mac) as uv
            |from log_data
            |group by getProductBrand(productModel)
          """.stripMargin)
          .collect



        withCsvWriterOld("/tmp/productbrandvv1.csv") {
          out => {
            monthproducthuv.foreach(e => {
              out.print(e.getString(0), e.getLong(1))
            })
          }
        }
      }
    }
  }
}
