package com.moretv.bi.report.medusa.userDevelop

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, ProductModelUtils}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 11/28/16.
  */

/**
  * 维度：终端型号，渠道，时间
  *
  * 数据源：loginlog
  */
object ChannelProductActiveStat extends BaseClass {

  private val tableName = "channel_product_intervals_stat"

  private val fields = "interval_type,intervals,productModel,promotionChannel,pv,uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where interval_type =? and intervals = ?"

  def main(args: Array[String]) {

    ModuleClass.executor(ChannelProductActiveStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        // init & util
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w => {
          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          val loadPath = s"/log/moretvloginlog/parquet/$loadDate/loginlog"

          sqlContext.read.parquet(loadPath)
            .filter(s"date between '$sqlDate' and '$sqlDate'")
            .select("productModel", "promotionChannel", "userId")
            .registerTempTable("log_data")

          val df = sqlContext.sql(
            """
              |select productModel, promotionChannel, count(userId) as pv, count(distinct userId) as uv
              |from log_data
              |group by productModel, promotionChannel
            """.stripMargin)

          util.delete(deleteSql, "day", sqlDate)

          df.collect.foreach(w => {

            val productModel = ProductModelUtils.getProductBrand(w.getString(0))
            val promotionChannel = w.getString(1)
            val pv = new JLong(w.getLong(2))
            val uv = new JLong(w.getLong(3))

            util.insert(insertSql, "day", sqlDate, productModel, promotionChannel, pv, uv)
          })


        })
      }
      case None => {

      }
    }
  }
}
