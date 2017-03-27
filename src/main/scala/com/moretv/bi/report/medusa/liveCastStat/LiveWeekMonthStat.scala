package com.moretv.bi.report.medusa.liveCastStat

import java.util.Calendar

import com.moretv.bi.util.baseclasee.BaseClass
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 3/24/17.
  */
object LiveWeekMonthStat extends BaseClass {

  def main(args: Array[String]) {

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        val q = sqlContext

        val cal = Calendar.getInstance

        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))



      }
      case None => {

      }
    }

  }
}
