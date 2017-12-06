package com.moretv.bi.realtimeStatistics

import com.moretv.bi.util.{ConfigureBuilder, Params, ParamsParseUtil, QuartzUtils}
import org.quartz.{Job, JobExecutionContext}


/**
 * Created by zhangyu on 17/11/29.
 * 获取长连接下的分版本实时在线人数(每隔整点五分钟获取该区间的五个数,取平均值)
 * doc:http://doc.whaley.cn/pages/viewpage.action?pageId=5345232
 * table: medusa_keep_alive_client_online_version_distribution(id,start_time,end_time,version,client_online)
 */
object keepAliveTotalClientOnlineVersionDist {

  //主要参数
  private val url = "/statistic/countByAppVersion"
  private val domain = "lcbi.aginomoto.com"
  private val product = "moretv"
  val getUrl = "http://" + domain + url + "?product=" + product

  private val tableName = "medusa_keep_alive_client_online_version_distribution"
  //private val deleteSql = s"delete from $tableName where time = ?"
  private val insertSql = s"insert into $tableName(start_time,end_time,version,client_online) values(?,?,?,?)"


  class MyJob extends Job {
    var p:Params = null
    override def execute(jobExecutionContext: JobExecutionContext) = {
      getData(p)
    }
  }


  def main(args: Array[String]) = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        println(p.calculateNum)
        val job:MyJob = new MyJob()
        job.p = p
        val configure = new ConfigureBuilder(p.sleepTime, p.retryNum, p.calculateNum)

        val quartz = new QuartzUtils
        quartz.setJob(job)
        quartz.setConfigure(configure)
        quartz.execute

      }
      case None => {
        throw new RuntimeException("At least one param -- crontab !")
      }
    }
  }

  def getData(p:Params): Unit = {
    if (p == null) println("HHHHHHHH") else print(p.calculateNum)
  }


}




//  lazy val quartz = new StdSchedulerFactory().getScheduler
//  private var sleepTime = 0L
//  private var calculateNum = 0
//
//  private val retryNum = 3
//
//  //每隔5分钟起一次调度
//

//
//  def main(args: Array[String]): Unit = {
//
//    ParamsParseUtil.parse(args) match {
//      case Some(p) => {
//        val Job = new MyJob
//
//        val job = JobBuilder.newJob(classOf[MyJob])
//          .withIdentity("Job", "Group")
//          .build
//
//        val trigger: Trigger = TriggerBuilder
//          .newTrigger
//          .withIdentity("Trigger", "Group")
//          .withSchedule(
//            CronScheduleBuilder.cronSchedule(p.crontab))
//          .build
//
//        sleepTime = p.sleepTime
//        calculateNum = p.calculateNum
//
//        quartz.start
//
//        try {
//          quartz.scheduleJob(job, trigger)
//
//        } catch {
//          case e: Exception => quartz.shutdown()
//        }
//
//      }
//      case None => {
//        throw new RuntimeException("At least needs one param: sleepTime!")
//      }
//    }
//  }
//
//
//  //轮询模块,存储数据,存储五个后计算结果
//  def getData(): Unit = {
//
//    //存储五个数据,形式为(版本-> (时间,人数))
//    val fiveDataMap = scala.collection.mutable.Map[String,ListBuffer[(String,Long)]]()
//    val timeSet = scala.collection.mutable.Set[(String)]()
//
//
//    (0 until 5).foreach(i => {
//      try {
//        val json = HttpUtils.get(getUrl, retryNum)
//        //获取接口数据并解析
//        if (json != null) {
//          val jsonObject = new JSONObject(json)
//          val status = jsonObject.getString("status")
//          if (status == "200") {
//            val jsonArray = jsonObject.getJSONArray("data")
//
//            (0 until jsonArray.length()).foreach(i => {
//              val innerObj = jsonArray.getJSONObject(i)
//              val app_version = innerObj.getString("app_version")
//              val time = innerObj.getString("time")
//              val clientNum = innerObj.getString("client_online")
//              timeSet += time
//
//              if (fiveDataMap.contains(app_version)) {
//                val value = fiveDataMap(app_version)
//                value.+=((time, clientNum.toLong))
//              } else {
//                val value = new ListBuffer[(String, Long)]()
//                value.+=((time, clientNum.toLong))
//                fiveDataMap += (app_version -> value)
//              }
//            })
//          }
//
//         // println(timeSet.size)
//          println(timeSet)
//          if (timeSet.size == 5) {
//           // val time = timeSet.sortWith((s, t) => s.compareTo(t) < 0)
//            val startTime = timeSet.min
//            val endTime = timeSet.max
//            println(startTime + "///////" + endTime)
//
//            val clientNumList = ListBuffer[(String,Long)]() //存储版本及人数和
//            val keyList = fiveDataMap.keySet.toList //版本键集合
//
//            //计算各版本五分钟的人数和
//            (0 until keyList.length).foreach(i => {
//              val app_version = keyList(i)
//              val value = fiveDataMap(app_version).toList
//              var clientOnline = 0L
//              (0 until value.length).foreach(i => {
//                clientOnline += value(i)._2
//              })
//              clientNumList.+=((app_version, clientOnline))
//            })
//
//            val util = DataIO.getMySqlOps(DataBases.STREAMING_BI_MYSQL)
//
//            (0 until clientNumList.length).foreach(i => {
//              val version = clientNumList(i)._1
//              val clientSum = clientNumList(i)._2
//              //println(version + ":  " + clientSum)
//              util.insert(insertSql, TimeUtil.floorMinute(startTime), TimeUtil.ceilMinute(endTime), version,
//                (clientSum * 1.0 / 5).toLong)
//            })
//
//            util.destory()
//            fiveDataMap.clear()
//            timeSet.clear()
//          }
//
//        }
//        Thread sleep 60000
//      } catch {
//        case e: Exception => {
//          val msg = e.getMessage
//          println(msg)
//          throw e
//        }
//      }
//    })
//  }


