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
object CommonSubTotalPVUV extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("CommonSubTotalPVUV")
    ModuleClass.executor(CommonSubTotalPVUV,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/interview/"+p.startDate
        val df = sqlContext.read.load(path)
        val resultRDD = df.filter("event = 'enter'").select("date","path","userId").
          map(e =>(e.getString(0),e.getString(1),e.getString(2))).
          map(e=>(getKeys(e._1,e._2),e._3)).
          filter(_ != null).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from commonSubTotalPVUV where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO commonSubTotalPVUV(year,month,day,weekstart_end,module,subModule,subModuleName,user_num,access_num) VALUES(?,?,?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          try{
            util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,x._1._7,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
          }catch {
            case e:Exception =>
          }
        })

        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def judgePath(path:String) = {

  }

  def getKeys(date:String, path:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)


    val subject = List("movie","tv","zongyi","comic","kids","jilu","mv","hot","xiqu","sports","kids_home")
    val array = path.split("-")
    if(array.length == 3 && subject.contains(array(1)) && "home".equalsIgnoreCase(array(0))){
      val module = array(1)
      val submodule = array(2)
      val moduleName = CodeToNameUtils.getThirdPathName(submodule)
      (year,month,date,week,module,submodule,moduleName)
    }else null

  }
}
