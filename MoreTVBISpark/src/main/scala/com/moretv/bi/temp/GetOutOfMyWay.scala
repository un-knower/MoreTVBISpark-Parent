package com.moretv.bi.temp

import java.util

import com.moretv.bi.util.ParamsParseUtil
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

/**
  * Created by witnes on 10/28/16.
  */
object GetOutOfMyWay extends BaseClass {


  def main(args: Array[String]) {

    ModuleClass.executor(this,args)

  }


  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil parse (args) match {
      case Some(p) => {
        val load2 =
          "/mbi/parquet/playview/{201508*,201509*,201510*,201511*,201512*,201601*,201602*,2016030*,2016031*,20160320,20160321}"

        val load3 = "/log/medusaAndMoretvMerger/2016{032*,04*,05*,06*,07*,08*,09*,10*,1101}/playview"

        val df2 = sqlContext.read.parquet(load2)
          .filter("date between '2015-08-01' and '2016-03-20'")
          .filter("path like '%hotrecommend%'")
          .filter("event in ('playview','startplay')")
          .select("date", "userId", "videoSid")

        val df3 = sqlContext.read.parquet(load3)
          .filter("date between '2016-03-21' and '2016-10-31'")
          .filter("pathMain like '%recommendation%' or path like '%hotrecommend%'")
          .filter("event in ('playview','startplay')")
          .select("date", "userId", "videoSid")

        df2.unionAll(df3).registerTempTable("log_data")

        val dfOut = sqlContext.sql(
          s"""
             |select substr(date,1,7), count(userId)/count(distinct date) as pv, count(distinct userId)/count(distinct date) as uv,count(userId)/count(distinct userId) as ratio
             |from log_data
             |group by substr(date,1,7)
          """.stripMargin)

        dfOut.map(e => (e.getString(0), e.getDouble(1), e.getDouble(2), e.getDouble(3))).saveAsTextFile(p.outputFile)
      }

      case None => {

      }
    }


  }


  def parseCmd(logType: String, events: Array[String]) = {}


}
