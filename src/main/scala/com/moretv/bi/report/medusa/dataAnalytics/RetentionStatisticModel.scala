package com.moretv.bi.report.medusa.dataAnalytics

import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.utils.Params
import com.moretv.bi.global.LogTypes
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by xiajun on 2017/7/31.
  * 该类用于统计留存率相关的模板
  */
object RetentionStatisticModel {


  /**
    * 该函数用于统计留存率
    * @param sqlC：表示SQLContext
    * @param pMap
    * @param dimensions：表示统计过程中所涉及到的维度信息
    * @param measure：统计度量字段
    * @param date：传递的日期信息
    * @param userDF：参考的用户List
    * @return
    */
  def retentionByBehavior(sqlC:SQLContext, pMap:Map[String,String], measure:String, date:String, userDF:DataFrame, dimensions:Array[String]=Array()):Tuple4[Long,Long,DataFrame,DataFrame] = {

    // 拼接字段信息
    var fieldStr = measure
    if (!dimensions.isEmpty){
      fieldStr = measure.concat(",").concat(dimensions.mkString(","))
    }

    DataIO.getDataFrameOps.getDF(sqlC, pMap, "loginlog", LogTypes.LOGINLOG, date).registerTempTable("behavior_log")

    val behaviorDF = sqlC.sql(
      s"""
        |select ${fieldStr}
        |from behavior_log
      """.stripMargin)

    val retentionDF = userDF.intersect(behaviorDF).distinct()

    val userDFCount = userDF.count()

    val retentionDFCount = retentionDF.count()

    if(!dimensions.isEmpty){
      val userDFCountByDim = userDF.groupBy("\"".concat(dimensions.mkString("\",\"").concat("\""))).count()

      val retentionDFCountByDim = retentionDF.groupBy("\"".concat(dimensions.mkString("\",\"").concat("\""))).count()

      val resultTuple = (userDFCount,retentionDFCount,userDFCountByDim,retentionDFCountByDim)

      return resultTuple
    }else{
      val resultTuple = (userDFCount,retentionDFCount,null,null)
      return resultTuple
    }

  }

}
