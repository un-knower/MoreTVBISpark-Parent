package com.moretv.bi.etl

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.report.medusa.channelClassification.ChannelClassificationStatETL._
import com.moretv.bi.util.HdfsUtil
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
  * Created by baozhi.wang on 2017/4/17.
  *
  * 方便协同开发与代码结构清晰
  */

object PlayViewETLUtil  {

//kids etl
def kidsETL(sqlContext:SQLContext,dfMap:Map[String,DataFrame],mapflag:String): DataFrame = {
  var result:DataFrame=null
  val fact_df=dfMap.get("fact").get
  fact_df.registerTempTable("fact")





  result
}

//sport etl

//mv etl

//other etl

//filter 5000

/**统计DataFrame,或者表的记录条数*/
def dfRecordCount(sqlContext:SQLContext,df:DataFrame,tableName:String,isDebug:Boolean): Long = {
  var count=0l
  if(null!=df){
    count=df.count()
  }else{
    val df_result=sqlContext.sql(s"select count(1) as total_count from $tableName ")
    count=df_result.collect().head.getLong(0)
  }
  count
}

  /**用来写入HDFS，测试数据是否正确*/
  def writeToHDFSForCheck(date: String, logType: String, df: DataFrame, isDeleteOld: Boolean,isDebug:Boolean): Unit = {
    if (isDebug) {
      println(s"--------------------$logType is write done.")
      val outputPath = DataIO.getDataFrameOps.getPath(MERGER, logType, date)
      if (isDeleteOld) {
        HdfsUtil.deleteHDFSFile(outputPath)
      }
      df.write.parquet(outputPath)
    }
  }

}
