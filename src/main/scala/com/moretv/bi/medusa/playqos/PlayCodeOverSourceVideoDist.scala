package com.moretv.bi.medusa.playqos

import java.util.Calendar

import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 4/3/17.
  */
object PlayCodeOverSourceVideoDist extends BaseClass {


  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val sq = sqlContext
        import org.apache.spark.sql.functions._
        import sq.implicits._

        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        val date = DateFormatUtils.readFormat.format(cal.getTime)

        (0 until p.numOfDays).foreach(i => {
          // readPath = s"/log/medusa/parquet/$date/playqos"


        })
      }
      case None => {

      }
    }
  }
}
