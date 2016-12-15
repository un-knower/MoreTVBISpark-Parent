package com.moretv.bi.medusa.temp

import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 9/8/16.
  */
object MiboxPlayTrend extends BaseClass{

  private val tableName = "medusa_temp_mibox_trend"

  private val filterStr = "MiBOX3_PRO,MiBOX_mini"


//  def main(args: Array):Unit ={
//
//
//  }
//

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i =>{
          val date = DateFormatUtils.readFormat.format(cal.getTime)

          cal.add(Calendar.DAY_OF_MONTH,-1)
        })
      }
      case None => {
        throw new RuntimeException("at least needs one param: startDate")
      }
    }
  }

  def isContained(field:String):Boolean ={

    val filterArr = filterStr.split(",")
    filterArr.contains(field)

  }
}
