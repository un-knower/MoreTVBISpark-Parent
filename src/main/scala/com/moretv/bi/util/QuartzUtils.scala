package com.moretv.bi.util

import org.quartz._
import org.quartz.impl.StdSchedulerFactory

/**
 * Created by zhangyu on 17/11/30.
 */
class QuartzUtils {

  var configure:ConfigureBuilder = null

  var job:Job = null

  lazy val quartz = new StdSchedulerFactory().getScheduler

  def setJob(job:Job) = {
    this.job = job
  }

  def setConfigure(configure:ConfigureBuilder) = {
    this.configure = configure
  }

  def execute = {
    if(job == null || !job.isInstanceOf[Job]) {
      throw new RuntimeException("job must instance!")
    }

    val jobDetail = JobBuilder.newJob(job.getClass)
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
      quartz.scheduleJob(jobDetail, trigger)

    } catch {
      case e: Exception => quartz.shutdown()
    }
  }



}
