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


  private val tableName1 = "moretv_cumulative_user_productmodel_dist"

  private val fields1 = "day,productmodel,user_num"

  private val insertSql1 = s"insert into $tableName1($fields1)values(?,?,?)"

  private val tableName2 = "moretv_new_user_province_dist"

  private val fields2 = "year,province,user_num"

  private val insertSql2 = s"insert into $tableName2($fields2)values(?,?,?)"


  def main(args: Array[String]) {

    ModuleClass.executor(CumulativeUserDist, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val q = sqlContext
        import q.implicits._

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val getProvince = udf((e: String) => IPLocationDataUtil.getProvince(e))

        // cummulative User Product_Model Dist
        val userProductModelDataset =
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, "20170101")
            .filter(to_date($"openTime") < "2017-01-01" && $"mac".isNotNull && $"product_model".isNotNull)
            .select("mac", "product_model")
            .groupBy("product_model")
            .agg(countDistinct($"mac"))
            .collect

        val userProvinceDataSet =
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, "20170101")
            .filter(
              to_date($"openTime").between("2016-01-01", "2016-12-31")
                && $"mac".isNotNull
                && $"ip".isNotNull
            )
            .select($"mac", getProvince($"ip").as("province"))
            .groupBy("province")
            .agg(countDistinct($"mac"))
            .collect


        userProvinceDataSet.foreach(w => {
          util.insert(insertSql2, "2016", w.getString(0), w.getLong(1))
        })

        userProductModelDataset.foreach(w => {
          util.insert(insertSql1, "2016-12-31", w.getString(0), w.getLong(1))
        })


      }
      case None => {

      }
    }

  }

}
