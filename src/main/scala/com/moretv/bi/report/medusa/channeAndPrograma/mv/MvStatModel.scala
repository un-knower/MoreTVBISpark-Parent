package com.moretv.bi.report.medusa.channeAndPrograma.mv

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.DataFrame
import org.joda.time.format.DateTimeFormat

/**
  * Created by witnes on 4/1/17.
  */
abstract class MvStatModel extends BaseClass {

  private val contentType = "mv"

  val readDateFormat = DateTimeFormat.forPattern("yyyyMMdd")


  var logType: String = _

  var pathPattern: String = _


  var tbl: String = ""

  var fields: Seq[String] = Array.empty[String]

  def aggUserStat(ods: DataFrame): DataFrame

  def joinStatWithDim(ods: DataFrame): DataFrame

  def outputHandle(
                    agg: DataFrame, deleteOld: Boolean, readDate: String, util: MySqlOps,
                    deleteSql: String, insertSql: String
                  ): Unit


  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        var readDate = readDateFormat.parseLocalDate(p.startDate)

        val insertSql =
          s"insert into $tbl (${fields.mkString(",")}) values(${List.fill(fields.length)("?").mkString(",")})"

        val deleteSql = s"delete from $tbl where day = ?"


        (0 until p.numOfDays).foreach(d => {


          //ETL
          val ods = MvHomePageEntranceForm.MvHomeEntranceIdFrom(contentType, p.paramMap,

            DataIO.getDataFrameOps.getDF(
              sc, p.paramMap, pathPattern, logType, readDate.toString("yyyyMMdd")
            )

          )

          //Aggregation
          val aggOds = aggUserStat(ods)

          //Join with Dim
          val aggRes = joinStatWithDim(aggOds)

          outputHandle(aggRes, p.deleteOld, readDate.toString("yyyy-MM-dd"), util, deleteSql, insertSql)

          readDate.minusDays(1)
        })


      }
      case None => {

      }
    }

  }

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }
}
