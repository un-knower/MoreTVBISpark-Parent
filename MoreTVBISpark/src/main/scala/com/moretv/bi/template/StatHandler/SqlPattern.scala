package com.moretv.bi.template.StatHandler

/**
  * Created by witnes on 11/2/16.
  */
object SqlPattern extends CommonSqlTrait {


  def defineSql: String = {
    s"""
       |select $selectFields
       |from log_data as tbl1
       |join log_ref as tbl2
       |on tbl1.$joinField = tbl2.$joinField
       |where event in ($events)
       |and $timeType between tbl1.startTime and tbl2.endTime
       |group by $groupFields
    """.stripMargin

  }


}

