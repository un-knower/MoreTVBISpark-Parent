package com.moretv.bi.temp.user

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/3/21.
  */
object ApkVersionUserTrend extends SparkSetting{

  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    val inputPath = args(0)

    val logRdd = sqlContext.read.load(inputPath).select("version","mac","date").
      map(row => {
        val version = row.getString(0)
        if(version != null && (version.contains("_TVApp2.0_") || version.contains("Medusa"))){
          val idx = version.lastIndexOf("_")
          ((row.getString(2),version.substring(idx+1)),row.getString(1))
        }else null
      }).
      filter(_ != null).cache()
    logRdd.distinct().countByKey().foreach(e => println(e._1._1 + "," + e._1._2 + "," + e._2))

  }

}
