package com.moretv.bi.history

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
object History_PVUVVV extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("History_PVUVVV")
    ModuleClass.executor(History_PVUVVV,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {


        //calculate log whose type is play
        val path = "/mbi/parquet/{playview,detail}/" + p.startDate + "/part-*"
        val df = sqlContext.read.load(path).persist(StorageLevel.MEMORY_AND_DISK)
        val playRDD = df.filter("apkVersion > '2.4.7' and logType='playview' and event ='playview'").select("date", "path", "contentType", "userId").map(e => (e.getString(0), e.getString(1), e.getString(2), e.getString(3))).
          filter(e => judgePath(e._2)).map(e => (getKeys(e._1, e._2, e._3), e._4)).filter(e => e._1 != null).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_play = playRDD.distinct().countByKey()
        val accessNum_play = playRDD.countByKey()

        val detailRDD = df.filter("apkVersion > '2.4.7' and logType='detail'").select("date", "path", "contentType", "userId").map(e => (e.getString(0), e.getString(1), e.getString(2), e.getString(3))).
          filter(e => judgePath(e._2)).map(e => (getKeys(e._1, e._2, e._3, "detail", ""), e._4)).filter(e => e._1 != null).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum_detail = detailRDD.distinct().countByKey()
        val accessNum_detail = detailRDD.countByKey()

        //save date
        val util = new DBOperationUtils("bi")
        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from history_pvuvvv where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO history_pvuvvv(year,month,day,weekstart_end,type,exit_type,module_code,module,uv_num,vv_num) VALUES(?,?,?,?,?,?,?,?,?,?)"
        userNum_play.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,x._1._7,x._1._8,new Integer(x._2.toInt),new Integer(accessNum_play(x._1).toInt))
        })

        userNum_detail.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,x._1._7,x._1._8,new Integer(x._2.toInt),new Integer(accessNum_detail(x._1).toInt))
        })

        playRDD.unpersist()
        detailRDD.unpersist()
        df.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }

  }

  def judgePath(path:String) = {
    val subject = List("history","collect","mytag")
    val array = path.split("-")
    if(array.length == 3 && subject.contains(array(2)) && "history".equalsIgnoreCase(array(1))){
      true
    }else if(array.length == 5 && subject.contains(array(2)) && "history".equalsIgnoreCase(array(1))){
      val reg = "subject-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+|hot\\d+)".r
      val pattern = reg findFirstMatchIn path
      val res = pattern match {
        case Some(x) => true
        case None => false
      }
      res
    }else{
      false
    }
  }

  def getKeys(date:String, path:String, contentType:String, logType:String = "play", exitType:String = "playview")={
    //obtain time
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(date))
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH)+1
    val week = getWeekStartToEnd(date)

    //obtain module
    val moduleToName = Map("history"->"观看历史","tv"->"在追的剧","movie"->"电影收藏","zongyi"->"综艺收藏","jilu"->"纪实收藏","kids"->"少儿收藏","comic"->"动漫收藏","hot"->"咨询短片","mv"->"音乐","xiqu"->"戏曲","sports"->"体育","subject"->"专题收藏","tag"->"我的订阅")
    val array = path.split("-")
    var module = array(2)
    var title = moduleToName.getOrElse(module,"")
    if(title == ""){
      module = if(array.length == 5) array(3) else contentType
      title = moduleToName.getOrElse(module,"")
    }
    if (title != ""){
      (year,month,date,week,logType,exitType,module,title)
    }else null
  }
}
