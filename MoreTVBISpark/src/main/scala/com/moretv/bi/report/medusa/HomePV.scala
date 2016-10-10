package com.moretv.bi.report.medusa

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/4/18.
 */
object HomePV extends SparkSetting{


  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)
    sqlContext.read.load("/log/medusa/parquet/*/enter").registerTempTable("log_data")
    val result = sqlContext.sql("select date,count(distinct userId),count(userId) from log_data group by date").collect()

    result.foreach(row => println(row.getString(0) + "," + row.getLong(1) + "," + row.getLong(2)))
  }

}
