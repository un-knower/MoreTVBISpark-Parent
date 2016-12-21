package com.moretv.bi.combine

import java.util.Calendar

import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 11/16/16.
  */
object ISPAreaPromotionStat extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }


  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(
          w =>{

            val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
            cal.add(Calendar.DATE,-1)
            val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

            val loadPath = s"/log/medusaAndMoretvMerger/$loadDate/playview"

            sqlContext.read.parquet(loadPath)
              .filter(s"date = $loadDate")
              .filter("event in ('playview','startplay')")
              .select("ip","promotionChannel","userId")
              .registerTempTable("log_data")

            sqlContext.sql(
              """
                |
              """.stripMargin)

          }
        )

      }
      case None => {

      }
    }
  }
}
