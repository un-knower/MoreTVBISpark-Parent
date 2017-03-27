package com.moretv.bi.report.medusa.channeAndPrograma.mv

import com.moretv.bi.report.medusa.channeAndPrograma.mv.af310.SingerPlayStat._
import com.moretv.bi.report.medusa.util.udf.Path2Position
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 3/21/17.
  */
object PositionTst extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    val sq = sqlContext
    import sq.implicits._

    val entranceFromUdf = udf[Seq[String], String, String, Boolean](Path2Position.entranceFrom)

    val isRecommendedUdf = udf[Boolean, String](Path2Position.isRecommended)

    val pt = "/log/medusa/parquet/201703{1*}/play"

    sqlContext.read.parquet(pt)
      .filter($"contentType".isNotNull && $"event" === "startplay")
      .withColumn("pagePath", explode(split($"pathMain", "-")))
      .withColumn("pagePath", explode(split($"pathMain", "-")))
      .withColumn("entrance_code",
        explode(
          entranceFromUdf($"contentType", $"pagePath", isRecommendedUdf($"recommendType"))
        )
      )
      .select($"entrance_code", $"contentType").distinct
      .show(200, false)


  }
}
