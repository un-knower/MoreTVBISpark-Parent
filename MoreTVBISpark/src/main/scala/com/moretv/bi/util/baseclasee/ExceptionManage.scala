package com.moretv.bi.util.baseclasee


/**
 * 该类的作用是处理任务执行过程中出现的一些错误，并将错误信息保存与数据库中
 * Created by laishun on 16/8/10.
 */
object ExceptionManage {

  /**
   * job执行过程中出现错误时候，保存该错误信息到数据库中
   * @param className
   * @param e
   */
  def taskExceptionStore(className:String,e:Throwable,nowTime:String)={
    //查询该job Name是否已经执行
    val queryJob = TaskAndExceptionDao.getJobDetail(className,nowTime)
    if(queryJob != null && queryJob.length == 1 && queryJob(0).toString.toInt == 1){
      TaskAndExceptionDao.taskExceptionUpdate(className,e,"fail",nowTime)
    }else{
      TaskAndExceptionDao.taskExceptionStore(className,e,"fail",nowTime)
    }
  }

  /**
   * 任务执行情况
   * @param className
   */
  def taskExecuteStore(className:String,duration:Long,nowTime:String)={
    //查询该job Name是否已经执行
    val queryJob = TaskAndExceptionDao.getJobDetail(className,nowTime)
    if(queryJob != null && queryJob.length == 1 && queryJob(0).toString.toInt == 1){
      TaskAndExceptionDao.taskSuccessUpdate(className,"successful",nowTime,duration)
    }else{
      TaskAndExceptionDao.taskSuccessStore(className,"successful",duration,nowTime)
    }
  }

  /**
   * 任务执行情况
   * @param className
   */
  def taskRunStore(className:String,nowTime:String)={
    //查询该job Name是否已经执行
    val queryJob = TaskAndExceptionDao.getJobDetail(className,nowTime)
    if(queryJob != null && queryJob.length == 1 && queryJob(0).toString.toInt == 1){
      TaskAndExceptionDao.taskRunningUpdate(className,"running",nowTime)
    }else{
      TaskAndExceptionDao.taskRunningStore(className,"running",nowTime)
    }
  }

}
