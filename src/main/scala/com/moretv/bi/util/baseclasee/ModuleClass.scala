package com.moretv.bi.util.baseclasee

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.utils.SendMail
import com.moretv.bi.util.ParamsParseUtil

/**
 * Created by laishun on 16/8/8.
 */
object ModuleClass {

  private val emailArray = Array("xia.jun@whaley.cn")
  private val timeFormat = new SimpleDateFormat("yyyy-MM-dd")
  /**
   * Function:this method is a standard module to execute task
   * @param op
   */
  def executor(op:BaseClass,args:Array[String]):Unit={
    //initialize global parameters
    op.init()
    //obtain task execute time
    val executeTime = ParamsParseUtil.parse(args) match {
      case Some(p) => p.startDate
      case None    => getNowTime
    }
    val alarmFlag = ParamsParseUtil.parse(args) match {
      case Some(p) => p.alarmFlag
      case None => true
    }

    //task running
    ExceptionManage.taskRunStore(op.getClass.getName,executeTime)
    //execute task
    try {
      //任务start时间
      val start_time = System.currentTimeMillis()
      op.execute(args)
      //任务结束时间
      val end_time = System.currentTimeMillis()
      val duration = (end_time-start_time)/1000
      //保存执行结果
      ExceptionManage.taskExecuteStore(op.getClass.getName,duration,executeTime)
    }catch {
      case e:Throwable =>{
        e.printStackTrace()
        SendMail.post(e,"[北京机房][medusa]["+op.getClass.getName+"]["+executeTime+"]任务执行失败",emailArray)
        if(alarmFlag){
          ExceptionManage.taskExceptionStore(op.getClass.getName,e,executeTime)
        }
      }
    }finally{
      op.destroy()
    }
  }

  /**
   * 取得当前的时间
   * 格式：yyyy-MM-dd
   * @return
   */
  def getNowTime={
    val cal = Calendar.getInstance()
    timeFormat.format(cal.getTime)
  }
}
