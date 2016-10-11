package com.moretv.bi.common

import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2015/12/30.
 获取通过专题入口观看节目的pv，uv
 */

object SubjectPlayviewPVUV {
  def getPVUV(subject:String, sid:String, inputpath:String, sqlContext:SQLContext)={
    val df_init = sqlContext.read.parquet(inputpath)
    df_init.registerTempTable("log_data")
    val sql = "select userId from log_data where videoSid = '"+sid+"' and path like '%-"+subject+"%'"
    val df = sqlContext.sql(sql).persist()
    val pv = df.count()
    val uv = df.distinct().count()
    Array(pv,uv)
    df.unpersist()
  }
}
