package com.moretv.bi.activity

import java.io.{File, PrintWriter}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{GroupedData, DataFrame, SQLContext,Row}

object annualReviewReport extends BaseClass{

  def main(args: Array[String]) {
    config.setAppName("annualReviewReport")
    ModuleClass.executor(annualReviewReport,args)
  }
  override def execute(args: Array[String]) {
    val outFile = new File("/script/bi/moretv/zhehua/file/activity/annualReviewReport.csv")
    val out = new PrintWriter(outFile)
    val s = sqlContext
    import s.implicits._
    //val inputpath1 = "/log/activity/parquet/20160202/*"
    val inputpath = "/log/activity/parquet/2016020{2,3,4,5,6,7,8}/*"
    val df = sqlContext.read.parquet(inputpath).filter("activityId = 'i6stkmfhjlbc'").cache()

    out.println("********************首页点击人数次数********************")
    val df_1 = df.filter("logType = 'pageview' and event = 'view' and page = 'home'").select("date","userId")
    getDayPVUV(df_1).collect().foreach(out2File(_, out))
    out.print("活动期间首页点击总计PVUV:")
    getTotalPVUV(df_1).foreach(x => {
      out.print(x+"    ")
    })
    out.println()

    out.println("********************浏览每个页面的人数次数********************")
    val df_2 = df.filter("logType = 'pageview' and event = 'view' and page in ('page1','page2','page3','page4','page5'," +
      "'page6','page7','page8','page9','page10','page11','page12','page13','page14','page15')").select("date","page","userId")
    getDayPVUVByPage(df_2).sort($"page",$"date".asc).collect().foreach(out2File(_, out))
    out.print("活动期间浏览每个页面的总计PVUV:")
    getTotalPVUVByPage(df_2).collect().foreach(out2File(_, out))
    out.println()

    out.println("********************扫描二维码的人数次数********************")
    val df_3 = df.filter("logType = 'pageview' and event = 'view' and page = 'mobile'").select("date","userId")
    getDayPVUV(df_3).collect().foreach(out2File(_, out))
    out.print("活动期间扫描二维码的总计PVUV:")
    getTotalPVUV(df_3).foreach(x => {
      out.print(x+"    ")
    })
    out.println()

    out.println("********************给自己留言的人数次数********************")
    val df_4 = df.filter("logType = 'operation' and event = 'click' and page = 'submit'").select("date","userId")
    getDayPVUV(df_4).collect().foreach(out2File(_, out))
    out.print("活动期间给自己留言的总计PVUV:")
    getTotalPVUV(df_4).foreach(x => {
      out.print(x+"    ")
    })
    out.println()

    out.println("********************分享的人数次数********************")
    val df_5 = df.filter("logType = 'operation' and event = 'click' and page = 'share'").select("date","userId")
    getDayPVUV(df_5).collect().foreach(out2File(_, out))
    out.print("活动期间分享的总计PVUV:")
    getTotalPVUV(df_5).foreach(x => {
      out.print(x+"    ")
    })
    out.println()

    out.println("********************人均页面停留时长********************")
    val df_6 = df.filter("logType = 'duration' and event = 'exitActivity'").select("date","userId","duration").
      map(row => {
      val date = row.getString(0)
      val userId = row.getString(1)
      val duration = row.getString(2).toLong
      ((date,userId),duration)
    }).filter(e => e._2<1200 && e._2>0)
    val CountByKey = df_6.countByKey()
    val sumDurByDay = df_6.reduceByKey((x,y)=>x+y).map(e => {
      val key = e._1
      val sumDuration = e._2
      val sumCount = CountByKey(key)
      (key, sumDuration/sumCount)
    })

    val countByDay = sumDurByDay.map(e => (e._1._1,e._2)).countByKey()
    sumDurByDay.map(e => (e._1._1,e._2)).reduceByKey((x,y)=>x+y).map(e => {
      val date = e._1
      val sumDuration = e._2
      val sumCount = countByDay(date)
      (date, sumDuration/sumCount)
    }).collect.foreach(out.println(_))

    out.println("活动期间总的人均页面停留时长:")
    val sumCount = df_6.map(e => (e._1._2,e._2)).countByKey()
    val avgDurByUser = df_6.map(e => (e._1._2,e._2)).reduceByKey((x,y)=>x+y).map(e => {
      val userId = e._1
      val sumDurtion = e._2
      val count = sumCount(userId)
      sumDurtion/count
    })
    val avgDur = avgDurByUser.reduce((x,y)=>x+y)/avgDurByUser.count()
    out.println(avgDur)
    out.close()
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
