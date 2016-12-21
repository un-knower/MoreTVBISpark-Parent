package com.moretv.bi.medusa.enter

import com.moretv.bi.util.SparkSetting
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/4/18.
 */
object ActiveUser extends BaseClass{


  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    sqlContext.read.load("/log/medusa/parquet/*/enter").registerTempTable("log_data")
    val result = sqlContext.sql("select date,count(distinct userId),count(userId) from log_data group by date").collect()

    result.foreach(row => println(row.getString(0) + "," + row.getLong(1) + "," + row.getLong(2)))
  }

}
