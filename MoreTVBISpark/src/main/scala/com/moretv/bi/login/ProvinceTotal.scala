package com.moretv.bi.login

import java.lang.{Long => JLong}

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by Will on 2016/2/16.
  */
object ProvinceTotal extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val inputDate = p.startDate
//        val df = sqlContext.read.load(s"/log/dbsnapshot/parquet/$inputDate/moretv_mtv_account").
        val df = sqlContext.read.load(s"/log/dbsnapshot/parquet/$inputDate/helios_mtv_terminal").
          select("ip","serial_number").filter("serial_number is not null and ip is not null").distinct()
        val result = df.map(row => {
              val province = IPUtils.getProvinceByIp(row.getString(0))
              if(province != null) {
                province
              }else null
          }).filter(_ != null).countByValue()

        result.foreach(println)
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
