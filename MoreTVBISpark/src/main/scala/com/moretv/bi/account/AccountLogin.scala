package com.moretv.bi.account

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object AccountLogin extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("AccountLogin")
    ModuleClass.executor(AccountLogin,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/mtvaccount/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.filter("event='login'").select("date","userId","path").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
          map(e=>(getKeys(e._1,e._3),e._2)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from account_login_users where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO account_login_users(year,month,day,loginCode,loginName,user_num,access_num) VALUES(?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })

        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, path:String)={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(date))
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH)+1
    var eventName = ""
    if(path == "history")
      eventName ="从历史收藏登陆"
    else if(path == "setting")
      eventName ="从设置中登陆"
    else if(path == "addtag")
      eventName ="从标签中登陆"
    else if(path == "comment")
      eventName ="从评论中登陆"

    (year,month,date,path,eventName)
  }
}
