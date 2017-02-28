package com.moretv.bi.temp.annual

import org.apache.spark.sql._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._
import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by witnes on 1/10/17.
  */
object PromotionChannelAU extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val q = sqlContext
        import q.implicits._

        val loadPath = "/log/moretvloginlog/parquet/{2016*,20170101}/loginlog"

        val promotionPath = "/tmp/promotionchannel.csv"

        val newYearUserDb =
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, "20170101")
            .filter($"openTime".between("2016-01-01", "2016-12-31"))
            .select($"mac")


        val rdd = sc.textFile(promotionPath).map {
          attr => Row(attr)
        }

        val field = StructField("promotionChannel", StringType, nullable = true)

        val ref = sqlContext.createDataFrame(rdd, StructType(field :: Nil))

        ref.show(100, false)

        sqlContext.read.parquet(loadPath)
          .filter($"date" between("2016-01-01", "2016-12-31"))
          .filter("promotionChannel is not null and length(promotionChannel) <50")
          .select($"mac", $"promotionChannel")
          .groupBy($"promotionChannel".as("promotionChannel"))
          .agg(countDistinct($"mac").as("uv")).as("d")
          .join(ref.as("r"), $"r.promotionChannel" === $"d.promotionChannel")
          .select($"r.promotionChannel", $"d.uv")
          .show(500, false)


      }
      case None => {

      }

    }

  }
}
