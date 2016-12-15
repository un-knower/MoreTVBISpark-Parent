package com.moretv.bi.retention

import java.io.{PrintWriter, File}
import java.util.regex.Pattern

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Will on 2015/2/7.
 */
object ActivityRetentionRate  extends BaseClass{

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setMaster("spark://10.10.2.14:7077").
      setAppName("ActivityRetentionRate-活动留存情况").
      set("spark.executor.memory", "1g").
      set("spark.cores.max", "10").
      set("spark.scheduler.mode","FAIR")
    ModuleClass.executor(ActivityRetentionRate,args)
  }
  override def execute(args: Array[String]) {


    if(args.length < 3) {
      System.out.println("Need at least thre arguments!<activityId,firstDateStr,secondDateStr>")
      System.exit(1)
    }
    val pattern = Pattern.compile("/activity\\?log=operation-001-lotteryaction-"+args(0)+"-([A-Za-z0-9]{32})-lotteryaction")

    val yesterday = args(1)
    val today = args(2)

    val yesterdayUserRDD = sc.textFile("/log/activity/activity_"+yesterday+"*").map(line => matcherLog(line,pattern)).
                              filter(_ != null).distinct()
    val todayUserRDD = sc.textFile("/log/activity/activity_"+today+"*").map(line => matcherLog(line,pattern)).
      filter(_ != null).distinct()

    val result = yesterdayUserRDD.intersection(todayUserRDD).count()
    val yesterdayUser = yesterdayUserRDD.count()
    val todayUser = todayUserRDD.count()
    System.out.println("RetentionUser:\t"+result)
    System.out.println("RetentionUser:\t"+result)
    System.out.println("yesterdayUser:\t"+yesterdayUser)
    System.out.println("todayUser:\t"+todayUser)
    System.out.println("RetentionRate:\t"+result.toDouble / yesterdayUser)

    val file = new File("/home/moretv/liankai.tmp/share_dir/ActivityRetentionRate.txt")
    val out = new PrintWriter(file)
    out.println("RetentionUser:\t"+result)
    out.println("yesterdayUser:\t"+yesterdayUser)
    out.println("todayUser:\t"+todayUser)
    out.println("RetentionRate:\t"+result.toDouble / yesterdayUser)
    out.close()
  }

  def matcherLog(line:String,pattern: Pattern) = {
    val matcher = pattern.matcher(line)
    if(matcher.find()){
      matcher.group(1)
    }else null
  }
}
