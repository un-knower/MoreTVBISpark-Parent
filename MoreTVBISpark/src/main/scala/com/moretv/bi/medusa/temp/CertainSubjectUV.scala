package com.moretv.bi.medusa.temp

import java.util.Calendar

import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/12/16.
  */
object CertainSubjectUV extends  BaseClass{



  def main(args: Array[String]):Unit = {

    ModuleClass.executor(CertainSubjectUV, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        //date
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        //path
        val readPath = s"/log/Medusa/parquet/date"
        //df

        //rdd
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }



  }


}
