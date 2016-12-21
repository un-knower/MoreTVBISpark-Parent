package com.moretv.bi.activity

import java.io.{File, PrintWriter}

import com.moretv.bi.constant.Activity._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{LogUtils, SparkSetting}
import org.apache.spark.SparkContext


//
object ActivityNeiCeZhaoMu extends BaseClass{

  val SEPARATOR = ","

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    val inputPath = args(0)
    //val outputPath = args(1)

    val logRdd = sc.textFile(inputPath).filter(_.contains("activityId=i6s9t9l7ijmn")).map(log => LogUtils.log2json(log)).
      filter(_ != null)

    /*//专题内容页面每日UV、PV，活动期间总计UV、PV
    val rdd1 = logRdd.filter(json => (json.optString(LOG_TYPE) == PAGEVIEW && json.optString(PAGE) == "tv")).
         map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd1_1 = rdd1.map(_._2).cache()//总人数RDD
    val pvperday = rdd1.countByKey()
    val uvperday = rdd1.distinct().countByKey()
    val pvTotal = rdd1_1.count()
    val uvTotal = rdd1_1.distinct().count()
    val file1 = new File("/home/moretv/mbi/zhehua/file/activity1.csv")
    val out1 = new PrintWriter(file1)
    uvperday.foreach{
      x => out1.println(x._1+","+x._2+","+pvperday.get(x._1).get)
    }
    out1.println("专题内容页面活动期间总UV：   "+uvTotal)
    out1.println("专题内容页面活动期间总PV：   "+pvTotal)
    out1.close()

    //浏览第一页的单日人数、总人数(分渠道计算)
    val rdd2 = logRdd.filter(json => (json.optString(LOG_TYPE) == PAGEVIEW && json.optString(PAGE) == "welcome")).cache

    val rdd2_1 = rdd2.filter(json => json.optString(FROM) == "wx_menu").
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd2_11 = rdd2_1.map(_._2).cache()
    val pvperday2_1 = rdd2_1.countByKey()
    val uvperday2_1 = rdd2_1.distinct().countByKey()
    val pvTotal2_1 = rdd2_11.count()
    val uvTotal2_1 = rdd2_11.distinct().count()

    val rdd2_2 = rdd2.filter(json => json.optString(FROM) == "wx_msg").cache().
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd2_21 = rdd2_2.map(_._2).cache()
    val pvperday2_2 = rdd2_2.countByKey()
    val uvperday2_2 = rdd2_2.distinct().countByKey()
    val pvTotal2_2 = rdd2_21.count()
    val uvTotal2_2 = rdd2_21.distinct().count()

    val rdd2_3 = rdd2.filter(json => json.optString(FROM) == "m_tv").cache().
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd2_31 = rdd2_3.map(_._2).cache()
    val pvperday2_3 = rdd2_3.countByKey()
    val uvperday2_3 = rdd2_3.distinct().countByKey()
    val pvTotal2_3 = rdd2_31.count()
    val uvTotal2_3 = rdd2_31.distinct().count()

    val rdd2_4 = rdd2.filter(json => json.optString(FROM) == "m_bbs").cache().
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd2_41 = rdd2_4.map(_._2).cache()
    val pvperday2_4 = rdd2_4.countByKey()
    val uvperday2_4 = rdd2_4.distinct().countByKey()
    val pvTotal2_4 = rdd2_41.count()
    val uvTotal2_4 = rdd2_41.distinct().count()
    //注：FROM=m_tv情况下的统计结果和上面rdd1统计需求相同

    val file = new File("/home/moretv/mbi/zhehua/file/activity2.csv")
    val out2 = new PrintWriter(file)
    uvperday2_1.foreach{
      x => out2.println(x._1+","+x._2+","+pvperday2_1.get(x._1).get)
    }
    out2.println("来自微信固定入口活动期间总UV：   "+uvTotal2_1)
    out2.println("来自微信固定入口活动期间总PV：   "+pvTotal2_1)
    out2.println("******************************************")
    uvperday2_2.foreach{
      x => out2.println(x._1+","+x._2+","+pvperday2_2.get(x._1).get)
    }
    out2.println("来自微信阅读原文活动期间总UV：   "+uvTotal2_2)
    out2.println("来自微信阅读原文活动期间总PV：   "+pvTotal2_2)
    out2.println("******************************************")
    uvperday2_3.foreach{
      x => out2.println(x._1+","+x._2+","+pvperday2_3.get(x._1).get)
    }
    out2.println("来自TV端二维码活动期间总UV：   "+uvTotal2_3)
    out2.println("来自TV端二维码活动期间总PV：   "+pvTotal2_3)
    out2.println("******************************************")
    uvperday2_4.foreach{
      x => out2.println(x._1+","+x._2+","+pvperday2_4.get(x._1).get)
    }
    out2.println("来自论坛二维码活动期间总UV：   "+uvTotal2_4)
    out2.println("来自论坛二维码活动期间总PV：   "+pvTotal2_4)
    out2.println("******************************************")
    out2.close()*/


    //浏览至信息提交页的单日人数、总人数
    val rdd3 = logRdd.filter(json => json.optString(LOG_TYPE) == PAGEVIEW && json.optString(PAGE) == "register").cache()

    val rdd3_1 = rdd3.filter(json => json.optString(FROM) == "wx_menu").
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd3_11 = rdd3_1.map(_._2).cache()
    val pvperday3_1 = rdd3_1.countByKey()
    val uvperday3_1 = rdd3_1.distinct().countByKey()
    val pvTotal3_1 = rdd3_11.count()
    val uvTotal3_1 = rdd3_11.distinct().count()
    val file3 = new File("/home/moretv/mbi/zhehua/file/activity3.csv")
    val out3 = new PrintWriter(file3)
    uvperday3_1.foreach{
      x => out3.println(x._1+","+x._2+","+pvperday3_1.get(x._1).get)
    }
    out3.println("来自微信固定入口活动期间总UV：   "+uvTotal3_1)
    out3.println("来自微信固定入口活动期间总PV：   "+pvTotal3_1)
    out3.println("******************************************")

    val rdd3_2 = rdd3.filter(json => json.optString(FROM) == "wx_msg").
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd3_21 = rdd3_2.map(_._2).cache()
    val pvperday3_2 = rdd3_2.countByKey()
    val uvperday3_2 = rdd3_2.distinct().countByKey()
    val pvTotal3_2 = rdd3_21.count()
    val uvTotal3_2 = rdd3_21.distinct().count()
    uvperday3_2.foreach{
      x => out3.println(x._1+","+x._2+","+pvperday3_2.get(x._1).get)
    }
    out3.println("来自微信阅读原文活动期间总UV：   "+uvTotal3_2)
    out3.println("来自微信阅读原文活动期间总PV：   "+pvTotal3_2)
    out3.println("******************************************")

    val rdd3_3 = rdd3.filter(json => json.optString(FROM) == "m_tv").
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd3_31 = rdd3_3.map(_._2).cache()
    val pvperday3_3 = rdd3_3.countByKey()
    val uvperday3_3 = rdd3_3.distinct().countByKey()
    val pvTotal3_3 = rdd3_31.count()
    val uvTotal3_3 = rdd3_31.distinct().count()
    uvperday3_3.foreach{
      x => out3.println(x._1+","+x._2+","+pvperday3_3.get(x._1).get)
    }
    out3.println("来自TV端二维码活动期间总UV：   "+uvTotal3_3)
    out3.println("来自TV端二维码活动期间总PV：   "+pvTotal3_3)
    out3.println("******************************************")

    val rdd3_4 = rdd3.filter(json => json.optString(FROM) == "m_bbs").
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd3_41 = rdd3_4.map(_._2).cache()
    val pvperday3_4 = rdd3_4.countByKey()
    val uvperday3_4 = rdd3_4.distinct().countByKey()
    val pvTotal3_4 = rdd3_41.count()
    val uvTotal3_4 = rdd3_41.distinct().count()
    uvperday3_4.foreach{
      x => out3.println(x._1+","+x._2+","+pvperday3_4.get(x._1).get)
    }
    out3.println("来自论坛二维码活动期间总UV：   "+uvTotal3_4)
    out3.println("来自论坛二维码活动期间总PV：   "+pvTotal3_4)
    out3.println("******************************************")
    out3.close()



    //点击提交的人数单日人数、总人数
    val rdd4 = logRdd.filter(json => json.optString(LOG_TYPE) == "operation" && json.optString("event") == "submit").cache()

    val rdd4_1 = rdd4.filter(json => json.optString(FROM) == "wx_menu").
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd4_11 = rdd4_1.map(_._2).cache()
    val pvperday4_1 = rdd4_1.countByKey()
    val uvperday4_1 = rdd4_1.distinct().countByKey()
    val pvTotal4_1 = rdd4_11.count()
    val uvTotal4_1 = rdd4_11.distinct().count()
    val file4 = new File("/home/moretv/mbi/zhehua/file/activity4.csv")
    val out4 = new PrintWriter(file4)
    uvperday4_1.foreach{
      x => out4.println(x._1+","+x._2+","+pvperday4_1.get(x._1).get)
    }
    out4.println("来自微信固定入口活动期间总UV：   "+uvTotal4_1)
    out4.println("来自微信固定入口活动期间总PV：   "+pvTotal4_1)
    out4.println("******************************************")

    val rdd4_2 = rdd4.filter(json => json.optString(FROM) == "wx_msg").
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd4_21 = rdd4_2.map(_._2).cache()
    val pvperday4_2 = rdd4_2.countByKey()
    val uvperday4_2 = rdd4_2.distinct().countByKey()
    val pvTotal4_2 = rdd4_21.count()
    val uvTotal4_2 = rdd4_21.distinct().count()
    uvperday4_2.foreach{
      x => out4.println(x._1+","+x._2+","+pvperday4_2.get(x._1).get)
    }
    out4.println("来自微信阅读原文活动期间总UV：   "+uvTotal4_2)
    out4.println("来自微信阅读原文活动期间总PV：   "+pvTotal4_2)
    out4.println("******************************************")

    val rdd4_3 = rdd4.filter(json => json.optString(FROM) == "m_tv").
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd4_31 = rdd4_3.map(_._2).cache()
    val pvperday4_3 = rdd4_3.countByKey()
    val uvperday4_3 = rdd4_3.distinct().countByKey()
    val pvTotal4_3 = rdd4_31.count()
    val uvTotal4_3 = rdd4_31.distinct().count()
    uvperday4_3.foreach{
      x => out4.println(x._1+","+x._2+","+pvperday4_3.get(x._1).get)
    }
    out4.println("来自TV端二维码活动期间总UV：   "+uvTotal4_3)
    out4.println("来自TV端二维码活动期间总PV：   "+pvTotal4_3)
    out4.println("******************************************")

    val rdd4_4 = rdd4.filter(json => json.optString(FROM) == "m_bbs").
      map(json => (json.optString(DATE),json.optString(USER_ID))).cache()
    val rdd4_41 = rdd4_4.map(_._2).cache()
    val pvperday4_4 = rdd4_4.countByKey()
    val uvperday4_4 = rdd4_4.distinct().countByKey()
    val pvTotal4_4 = rdd4_41.count()
    val uvTotal4_4 = rdd4_41.distinct().count()
    uvperday4_4.foreach{
      x => out4.println(x._1+","+x._2+","+pvperday4_4.get(x._1).get)
    }
    out4.println("来自论坛二维码活动期间总UV：   "+uvTotal4_4)
    out4.println("来自论坛二维码活动期间总PV：   "+pvTotal4_4)
    out4.println("******************************************")
    out4.close()

    /*val rdd4_1 = rdd4.map(_._2).cache()
    val pvperday4 = rdd4.countByKey()
    val uvperday4 = rdd4.distinct().countByKey()
    val pvTotal4 = rdd4_1.count()
    val uvTotal4 = rdd4_1.distinct().count()
    val file4 = new File("/home/moretv/mbi/zhehua/file/activity4.csv")
    val out4 = new PrintWriter(file4)
    uvperday4.foreach{
      x => out4.println(x._1+","+x._2+","+pvperday4.get(x._1).get)
    }
    out4.println("点击提交的活动期间总UV：   "+uvTotal4)
    out4.println("点击提交的活动期间总PV：   "+pvTotal4)
    out4.close()*/

    /*def getperdayjsonrdd(jsonRdd:RDD[JSONObject]): RDD ={
      val rdd:RDD = jsonRdd.map(json => (json.optString(DATE),json.optString(USER_ID)))
    }
    def getTotaljsonrdd(jsonRdd:RDD[JSONObject]): RDD ={
      val rdd:RDD = jsonRdd.map(json => (json.optString(DATE),json.optString(USER_ID)))
    }*/

    /*withCsvWriterOld(outputPath){
      out => {
        out.println(new Date)
        pv.foreach(e => {
          val key = e._1
          val userNum = uv.get(key).get
          out.println(key+SEPARATOR+userNum+SEPARATOR+e._2)
        })
      }
    }
    rdd.unpersist()*/
  }
}
