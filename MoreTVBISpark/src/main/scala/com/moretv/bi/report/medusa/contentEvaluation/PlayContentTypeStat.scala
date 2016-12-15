package com.moretv.bi.report.medusa.contentEvaluation

import java.util.Calendar
import java.lang.{Long => JLong}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 10/12/16.
  */
object PlayContentTypeStat extends BaseClass {

  private val dataSource = "playview"

  private val tableName = "play_contenttype_stat"

  private val fields = "day, channel, pv, uv"

  private val insert1Sql = s"insert into $tableName ($fields) values(?,?,?,?)"

  private val delete1Sql = s"delete from $tableName  where day = ?"


  def main(args: Array[String]) {

    ModuleClass.executor(PlayContentTypeStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        // init
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w => {

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          val loadPath2 = s"/log/medusaAndMoretvMerger/$loadDate/$dataSource"

          val df2 = sqlContext.read.parquet(loadPath2)
            .select("userId", "contentType")
            .filter("contentType is not null")

          df2.registerTempTable("log_data")

          val dfPlay =
            sqlContext.sql(
              "select contentType, count(userId) as pv, count(distinct userId) as uv from log_data" +
                " group by contentType "
            )

          if(p.deleteOld){
            util.delete(delete1Sql, sqlDate)
          }

          dfPlay.collect.foreach(w=>{

            val contentType = w.getString(0)
            val pv = w.getLong(1)
            val uv = w.getLong(2)
            util.insert(insert1Sql, sqlDate, contentType, new JLong(pv), new JLong(uv))

          })

        })
      }
      case None => {
        throw new Exception("ContentTypeStat fails")
      }
    }
  }
}
