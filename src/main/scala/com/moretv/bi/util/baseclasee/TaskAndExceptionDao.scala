package com.moretv.bi.util.baseclasee

import java.lang.{Long => JLong}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.DBOperationUtils


/**
 * 该类的主要作用是对任务执行情况进行存储
 * Created by laishun on 16/5/12.
 */
object TaskAndExceptionDao {

  val db  = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
  println(db.prop.getProperty("url"))
  println(db.prop.getProperty("user"))
  println(db.prop.getProperty("password"))

  /**
   * job执行过程中出现错误时候，保存该错误信息到数据库中
   * @param className
   * @param e
   */
  def taskExceptionUpdate(className:String,e:Throwable,status:String,nowTime:String)={
    //存储每个任务的失败信息
    val insertSql = "update task_execute_history set execute_status = ?,exception = ? where class_name = ? and day = ?"
    db.insert(insertSql,status,ExceptionUtils.getErrorInfoFromException(e),className,nowTime)
  }

  /**
   * 任务执行情况
   * @param className
   * @param status
   */
  def taskExceptionStore(className:String,e:Throwable,status:String,nowTime:String)={//status:任务执行情况0表示成功，1表示失败，2表示恢复执行成功，3表示恢复执行失败
  //存储每个任务的信息
  val insertSql = "insert into task_execute_history(day,class_name,execute_status,exception) values(?,?,?,?)"
    db.insert(insertSql,nowTime,className,status,ExceptionUtils.getErrorInfoFromException(e))
  }

  /**
   * 更新任务执行情况
   * @param className
   * @param status
   */
  def taskSuccessUpdate(className:String,status:String,nowTime:String,duration:Long)={
    //存储每个任务的信息
    val insertSql = "update task_execute_history set execute_status = ?,exception = '',duration = ? where class_name = ? and day = ?"
    db.insert(insertSql,status,new JLong(duration),className,nowTime)
  }

  /**
   * 任务执行情况
   * @param className
   * @param status
   */
  def taskSuccessStore(className:String,status:String,duration:Long,nowTime:String)={//status:任务执行情况0表示成功，1表示失败，2表示恢复执行成功，3表示恢复执行失败
    //存储每个任务的信息
    val insertSql = "insert into task_execute_history(day,class_name,execute_status,duration) values(?,?,?,?)"
    db.insert(insertSql,nowTime,className,status,new JLong(duration))
  }

  /**
   * 任务开始执行情况
   * @param className
   * @param status
   */
  def taskRunningStore(className:String,status:String,nowTime:String)={//status:任务执行情况0表示成功，1表示失败，2表示恢复执行成功，3表示恢复执行失败
    //存储每个任务的信息
    val insertSql = "insert into task_execute_history(day,class_name,execute_status) values(?,?,?)"
    db.insert(insertSql,nowTime,className,status)
  }

  /**
   * 任务开始执行情况
   * @param className
   * @param status
   */
  def taskRunningUpdate(className:String,status:String,nowTime:String)={//status:任务执行情况0表示成功，1表示失败，2表示恢复执行成功，3表示恢复执行失败
    //存储每个任务的信息
    val insertSql = "update task_execute_history set execute_status = ?,exception = '' where day = ? and class_name = ?"
    db.insert(insertSql,status,nowTime,className)
  }

  /**
   * 根据任务名称和时间查询详细信息
   * @param className
   * @param day
   * @return
   */
  def getJobDetail(className:String,day:String)={
    val querySql = "select count(0) from task_execute_history where class_name=? and day = ?"
    db.selectOne(querySql,className,day)
  }
}
