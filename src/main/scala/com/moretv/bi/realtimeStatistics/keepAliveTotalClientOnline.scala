package com.moretv.bi.realtimeStatistics

import cn.whaley.bi.utils.HttpUtils
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.{ParamsParseUtil, TimeUtil}
import org.json.JSONObject
import org.quartz._
import org.quartz.impl.StdSchedulerFactory

import scala.collection.mutable.ListBuffer


/**
 * Created by zhangyu on 17/11/28.
 * 获取长连接下的实时在线人数(每隔整点五分钟获取该区间的五个数,取平均值)
 * doc:http://doc.whaley.cn/pages/viewpage.action?pageId=5345232
 * table: medusa_keep_alive_total_client_online(id,start_time,end_time,client_online)
 */
object keepAliveTotalClientOnline {

  //主要参数
  private val url = "/statistic/countTotal"
  private val domain = "lcbi.aginomoto.com"
  private val param = "moretv"
  val getUrl = "http://" + domain + url + "?product=" + param

  private val tableName = "medusa_keep_alive_total_client_online"
  //private val deleteSql = s"delete from $tableName where time = ?"
  private val insertSql = s"insert into $tableName(start_time,end_time,client_online) values(?,?,?)"

  //private var util:MySqlOps = null

  lazy val quartz = new StdSchedulerFactory().getScheduler
  //lazy val quartz = getDefaultScheduler

  //private val retryNum = 3
  private val getDataInterval = 5 //每隔5个数计算一次


  class MyJob extends Job {
    override def execute(jobExecutionContext: JobExecutionContext) = {
      getData()
    }
  }

  def main(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val Job = new MyJob

        val job = JobBuilder.newJob(classOf[MyJob])
          .withIdentity("Job", "Group")
          .build

        val trigger: Trigger = TriggerBuilder
          .newTrigger
          .withIdentity("Trigger", "Group")
          .withSchedule(
            CronScheduleBuilder.cronSchedule("0/30 * * * * ? *"))
          .build

        quartz.start

        try {
          quartz.scheduleJob(job, trigger)

        } catch {
          case e: Exception => quartz.shutdown()
        }

        //quartz.shutdown()
        //perMinute(getData, p.sleepTime)
      }
      case None => {
        throw new RuntimeException("At least needs one param: sleepTime!")
      }
    }
  }


  //轮询模块,存储数据,存储五个后计算结果
  def getData(): Unit = {
    var fiveDataList = new ListBuffer[(String, Long)]()

    (0 until 5).foreach(i => {
      try {
        val json = HttpUtils.get(getUrl, 5)
        if (json != null) {
          val jsonObject = new JSONObject(json)
          val status = jsonObject.getString("status")
          if (status == "200") {
            val innerJsonObj = jsonObject.getJSONObject("data")

            val time = innerJsonObj.getString("time")
            val clientOnline = innerJsonObj.getLong("client_online")
            val day = time.substring(0, 10)
            //println(time + " " +clientOnline)
            fiveDataList.+=((time, clientOnline))
          }

          if (fiveDataList.length == getDataInterval) {
            val fiveData = fiveDataList.toList
            val startTime = fiveData(0)._1
            val endTime = fiveData(fiveData.length - 1)._1
            var clientSum = 0L
            (0 until fiveData.length).foreach(j => {
              clientSum = clientSum + fiveData(j)._2.toLong
            })
            //              print("clientSum: " + clientSum.toString)
            //              println(startTime)
            //              println(endTime)

            val util = DataIO.getMySqlOps(DataBases.STREAMING_BI_MYSQL)
            util.insert(insertSql, TimeUtil.floorMinute(startTime), TimeUtil.ceilMinute(endTime), (clientSum * 1.0 / 5).toLong)
            util.destory()
            fiveDataList.clear()
          }
          Thread sleep 60000
        }
      } catch {
        case e: Exception => {
          val msg = e.getMessage
          println(msg)
          //util.destory()
          throw e
        }
      }
    })
  }




  //    //每分钟调用函数
  //    def perMinute(callback: () => Unit, second: Long): Unit = {
  //
  //      util = DataIO.getMySqlOps(DataBases.STREAMING_BI_MYSQL)
  //
  //      while (true) {
  //        callback()
  //        Thread sleep second
  //      }
  //      util.destory()
  //    }


}
