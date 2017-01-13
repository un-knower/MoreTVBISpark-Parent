package com.moretv.bi.temp.annual

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

/**
  * Created by witnes on 1/3/17.
  */

/**
  * 2016年累积用户 产品型号分布 ， 新增用户
  */
object CumulativeUserDist extends BaseClass {


  private val tableName1 = "moretv_cumulative_user_province_dist"

  private val fields1 = "end_date,province,user_num"

  private val insertSql1 = s"insert into $tableName1($fields1)values(?,?,?)"

  private val tableName2 = "moretv_new_user_province_dist"

  private val fields2 = "interval_type,intervals,area_type,area,user_num"

  private val insertSql2 = s"insert into $tableName2($fields2)values(?,?,?,?,?)"


  def main(args: Array[String]) {

    ModuleClass.executor(CumulativeUserDist, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val q = sqlContext
        import q.implicits._

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val userAreaDb = sqlContext.read.parquet("/log/medusa/userAreaInfo/20170105/userArea")


        // cummulative User Product_Model Dist
        //        val userProductModelDataset =
        //          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, "20170101")
        //            .filter(to_date($"openTime") < "2017-01-01" && $"mac".isNotNull && $"product_model".isNotNull)
        //            .select("mac", "product_model")
        //            .groupBy("product_model")
        //            .agg(countDistinct($"mac"))
        //            .collect


        DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, "20170101")
          .filter(to_date($"openTime") < "2017-01-01" && $"user_id".isNotNull)
          .as("d").join(userAreaDb.as("u"), $"d.user_id" === $"u.user_id")
          .select($"d.user_id", $"u.province")
          .groupBy($"u.province")
          .agg(count($"d.user_id"))
          .collect.foreach(w => {
          util.insert(insertSql1, "20161231", w.getString(0), w.getLong(1))
        })


        //        val userProvinceDataSet =
        //          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, "20170101")
        //            .filter(
        //              to_date($"openTime").between("2015-01-01", "2016-12-31")
        //                && $"user_id".isNotNull
        //            )
        //            .select($"user_id", year($"openTime").as("year"), month($"openTime").as("month"))
        //            .as("d").join(userAreaDb.as("u"), $"d.user_id" === $"u.user_id")
        //            .select(
        //              $"u.user_id".as("user_id"),
        //              $"d.year".as("year"),
        //              $"d.month".as("month"),
        //              $"u.province".as("province")
        //            )
        //            .groupBy($"year", $"month", $"province")
        //            .agg(count($"user_id"))


        //        userProvinceDataSet.collect.foreach(w => {
        //          util.insert(insertSql2, "month", w.getInt(0).toString + "-" + w.getInt(1).toString,
        //            "province", w.getString(2), w.getLong(3))
        //        })
        //
        //        userProductModelDataset.foreach(w => {
        //          util.insert(insertSql1, "2016-12-31", w.getString(0), w.getLong(1))
        //        })


      }
      case None => {

      }
    }

  }

}
