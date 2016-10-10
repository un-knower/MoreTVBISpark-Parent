package com.moretv.bi.operation

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
object ProgramEvaluateList extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("ProgramEvaluateList")
    ModuleClass.executor(ProgramEvaluateList,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/operation-e/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path).persist(StorageLevel.MEMORY_AND_DISK)
        val resultRDD = df.filter("event='evaluate'").select("date","videoSid","contentType","action","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4))).
                           map(e=>(getKeys(e._1,e._2,e._3,e._4),e._5)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = new DBOperationUtils("bi")
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from program_evaluate_list where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val redisUtil = new ProgramRedisUtils()
        val sql = "INSERT INTO program_evaluate_list(year,month,day,sid,title,content_type,operation,user_num,operate_num) VALUES(?,?,?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          val videoSid = x._1._4
          val title = getTitleBySid(redisUtil,videoSid)
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,title,x._1._6,x._1._7,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })
        resultRDD.unpersist()
        redisUtil.destroy()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, videoSid:String, contentType:String, action:String)={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(date))
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH)+1

    (year,month,date,videoSid,videoSid,contentType,action)
  }

  def getTitleBySid(util:ProgramRedisUtils,sid:String)={
    var title =""
    title = util.getTitleBySid(sid)
    if(title == "" || title == null)
      title = sid
    else if(title != null){
      title = title.replace("'", "");
      title = title.replace("\t", " ");
      title = title.replace("\r", "-");
      title = title.replace("\n", "-");
      title = title.replace("\r\n", "-");
    }
    title
  }
}
