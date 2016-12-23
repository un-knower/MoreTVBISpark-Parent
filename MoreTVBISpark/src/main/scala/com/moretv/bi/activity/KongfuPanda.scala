package com.moretv.bi.activity

import java.io.{File, PrintWriter}

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

@Deprecated
object KongfuPanda extends BaseClass{

  def main(args: Array[String]) {
    config.setAppName("KongfuPanda")
    ModuleClass.executor(KongfuPanda,args)
  }
  override def execute(args: Array[String]) {

    val outFile = new File("/script/bi/moretv/zhehua/file/activity/KongfuPanda_Whaley.csv")
    val out = new PrintWriter(outFile)
    val s = sqlContext
    import s.implicits._
    val inputpath = "/log/activity/parquet/201602{10,11,12,13,14,15}/*"
   // val df = sqlContext.read.parquet(inputpath).filter("activityId = 'i66j8qxyo8p8'").cache()
    val df = sqlContext.read.parquet(inputpath).filter("activityId = 'i6rsqse4jmhi'").cache()
    out.println("********************首页点击人数次数********************")
    val df_1 = df.filter("logType = 'pageview' and event = 'view' and page = 'home'").select("date","userId")
    //println(df_1.count())
    getDayPVUV(df_1).collect().foreach(out2File(_, out))
    out.print("活动期间首页点击总计PVUV:")
    getTotalPVUV(df_1).foreach(x => {
      out.print(x+"    ")
    })
    out.println()

    out.println("********************每个关卡的人数次数********************")
    val df_2 = df.filter("logType = 'operation' and event = 'click' and page in " +
      "('active_level1','active_level2','active_level3')").select("date","page","userId")
    getDayPVUVByPage(df_2).sort($"page",$"date".asc).collect().foreach(out2File(_, out))
    out.print("活动期间每个关卡的总计PVUV:")
    getTotalPVUVByPage(df_2).collect().foreach(out2File(_, out))
    out.println()

    out.println("********************到达抽奖页面的人数次数********************")
    val df_3 = df.filter("logType = 'operation' and event = 'click' and page = 'active_lottery'").select("date","userId")
    getDayPVUV(df_3).collect().foreach(out2File(_, out))
    out.print("活动期间到达抽奖页面的总计PVUV:")
    getTotalPVUV(df_3).foreach(x => {
      out.print(x+"    ")
    })
    out.println()

    out.println("********************参与抽奖的人数次数********************")
    val df_4 = df.filter("logType = 'pageview' and event = 'view' and " +
      "(page = 'fail' or page = 'prize')").select("date","userId")
    getDayPVUV(df_4).collect().foreach(out2File(_, out))
    out.print("活动期间参与抽奖的总计PVUV:")
    getTotalPVUV(df_4).foreach(x => {
      out.print(x+"    ")
    })
    out.println()

    out.println("********************中奖的人数次数********************")
    val df_5 = df.filter("logType = 'pageview' and event = 'view' and " +
      "page = 'prize'").select("date","userId")
    getDayPVUV(df_5).collect().foreach(out2File(_, out))
    out.print("活动期间中奖的总计PVUV:")
    getTotalPVUV(df_5).foreach(x => {
      out.print(x+"    ")
    })
    out.println()
    out.close
    df.unpersist()
  }

  // 获得每日PVUV
  def getDayPVUV(df:DataFrame) = {
    val day_PV = df.groupBy("date").count()//.collect()
    val day_UV = df.distinct().groupBy("date").count()
    day_PV.join(day_UV, "date")
  }
  def getDayPVUVByPage(df:DataFrame) = {
    val day_PV = df.groupBy("date","page").count//.collect()
    val day_UV = df.distinct().groupBy("date","page").count()
    day_PV.join(day_UV, Seq("date","page"))
  }
  //获得活动期间总的PVUV
  def getTotalPVUV(df:DataFrame): Array[Long] ={
    val total_PV = df.select("userId").count()
    val total_UV = df.select("userId").distinct().count()
    Array(total_PV,total_UV)
  }
  def getTotalPVUVByPage(df:DataFrame) ={
    val total_PV = df.select("page","userId").groupBy("page").count()
    val total_UV = df.select("page","userId").distinct().groupBy("page").count()
    total_PV.join(total_UV,"page")
  }
  def out2File(row:(Row), out:PrintWriter): Unit ={
    out.println(row)
  }

}
