package com.moretv.bi.report.medusa.util

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2016/4/14.
 */
object MedusaDataProcessModel {

  def calculatingPVUV(rdd:RDD[((Any,Any,Any,Any,Any,Any),Int)]):ArrayBuffer[Long]={
    val pv = rdd.count()
    val uv = rdd.distinct().count()
    var resultArr = new ArrayBuffer[Long]()
    resultArr += pv
    resultArr += uv
  }
}
