package com.moretv.bi.report.medusa

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Will on 2015/4/18.
 */
object TotalVVAndDuration extends SparkSetting{


  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.read.load("/log/medusa/parquet/*/play").registerTempTable("log_data")
    sqlContext.sql("select date,count(distinct userId),count(userId) from log_data group by date").
      collect().foreach(row => println(row.getString(0) + "," + row.getLong(1) + "," + row.getLong(2)))
    println()
    sqlContext.sql("select date,userId,duration from log_data").
      map(row => if(row.getString(2) != "" && row.getString(2) != "") {
        (row.getString(0),row.getString(1),row.getString(2).toInt)
      } else null).
      filter(_ != null).toDF.registerTempTable("log")

    val result = sqlContext.sql("select _1,sum(_3)/count(distinct _2) from log where _3 between 0 and 36000 group by _1").collect()

    result.foreach(row => println(row.getString(0) + "," + row.getDouble(1)))
  }

}
