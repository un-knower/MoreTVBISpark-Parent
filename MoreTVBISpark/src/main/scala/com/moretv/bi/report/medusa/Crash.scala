package com.moretv.bi.report.medusa

import java.lang.{Long => JLong}

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject

/**
  * Created by Will on 2016/3/23.
  */
object Crash extends SparkSetting{

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        val sqlContext = new SQLContext(sc)
        val inputDate = p.startDate

        val logRdd = sc.textFile(s"/log/crash/metadata/${inputDate}_extraction.log").map(log => {
          val json = new JSONObject(log)
          (json.optString("MAC"),json.optString("DATE_CODE"),json.optString("PRODUCT_CODE"))
        }).cache()
        println("total crash num:"+logRdd.count())
        println("total crash user:"+logRdd.distinct().count())
        val filterRdd = logRdd.map(x => (x._1.replace(":",""),x._2,x._3)).
          filter(x => !DevMacUtils.macFilter(x._1)).cache()
        println("昨天crash总次数:"+filterRdd.count()+" 总人数:"+filterRdd.distinct().count())

        val datecodeRdd = filterRdd.map(x => (x._2,x._1))
        val datecodeMap = datecodeRdd.countByKey()
        datecodeRdd.distinct().countByKey().foreach(e => {
          val key = e._1
          val crashUserNum = e._2
          val crashNum = datecodeMap(key)
          println(s"昨天DATE_CODE [] crash人数:${e._2}")
        })

        val productcodeRdd = filterRdd.map(x => (x._3,x._1))
        productcodeRdd.countByKey().foreach(e => println(s"昨天PRODUCT_CODE [${e._1}] crash次数:${e._2}"))
        productcodeRdd.distinct().countByKey().foreach(e => println(s"昨天PRODUCT_CODE [${e._1}] crash人数:${e._2}"))

        val codeRdd = filterRdd.map(x => ((x._2,x._3),x._1))
        codeRdd.countByKey().foreach(e => {
          println(s"昨天DATE_CODE [${e._1._1}] - PRODUCT_CODE [${e._1._2}] crash次数:${e._2}")
        })
        codeRdd.distinct().countByKey().foreach(e => {
          println(s"昨天DATE_CODE [${e._1._1}] - PRODUCT_CODE [${e._1._2}] crash人数:${e._2}")
        })

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
