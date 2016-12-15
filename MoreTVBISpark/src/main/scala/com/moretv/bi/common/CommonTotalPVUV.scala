package com.moretv.bi.common

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
object CommonTotalPVUV extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("CommonTotalPVUV")
    ModuleClass.executor(CommonTotalPVUV,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/interview/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.filter("event = 'enter'").select("date","path","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2))).filter(e =>judgePath(e._2)).
          map(e=>(getKeys(e._1,e._2),e._3)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from commonTotalPVUV where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO commonTotalPVUV(year,month,day,weekstart_end,module,user_num,access_num) VALUES(?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,new Integer(x._2.toInt),new Integer(accessNum.get(x._1).get.toInt))
        })
        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def judgePath(path:String) = {
    val subject = List("movie","tv","zongyi","comic","kids","jilu","mv","hot","xiqu","sports","kids_home","history","search")
    val array = path.split("-")
    if(array.length == 2 && subject.contains(array(1)) && "home".equalsIgnoreCase(array(0))){
      true
    }else{
      false
    }
  }

  def getKeys(date:String, path:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)

    val array = path.split("-")
    val module = array(1)

    (year,month,date,week,module)
  }
}
