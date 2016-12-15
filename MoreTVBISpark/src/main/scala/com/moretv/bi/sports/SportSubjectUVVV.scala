package com.moretv.bi.sports

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
object SportSubjectUVVV extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("SportSubjectUVVV")
    ModuleClass.executor(SportSubjectUVVV,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val path = "/mbi/parquet/playview/"+p.startDate+"/part-*"
        val df = sqlContext.read.load(path)
        val resultRDD = df.filter("path like 'home-sports%'").select("date","path","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
            map(e=>(getKeys(e._1,e._2),e._3)).filter(e =>(e._1._5 !=null && e._1._6!=null)).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from sports_subject_uv_vv where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO sports_subject_uv_vv(year,month,day,weekstart_end,parent_code,parent_name,subject_code,subject_name,uv_num,vv_num) VALUES(?,?,?,?,?,?,?,?,?,?)"
        val sectionList = CodeToNameUtils.getSportsSubsectionList()
        userNum.foreach(x =>{
          if(sectionList.contains(x._1._5)){
            val sectionName = CodeToNameUtils.getThirdPathName(x._1._5)
            val thirdPath = if(x._1._6 == "home_video") "home_video" else CodeToNameUtils.getParentNameBySubCode(x._1._6)
            val thirdPathName = if(thirdPath == "home_video") "首页视频" else CodeToNameUtils.getThirdPathName(thirdPath)
            if(sectionName != "null" && thirdPathName!= "null")
              util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,sectionName,thirdPath,thirdPathName,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
          }
        })

        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }


  def getKeys(date:String, path:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt
    val week = getWeekStartToEnd(date)

    var subSection:String = null
    var thirdPath:String  = null

    val regThird = "home-sports-(\\w+)-(\\w+)".r
    val pattern = regThird findFirstMatchIn path
    pattern match {
      case Some(x) =>
        subSection = x.group(1)
        val temp = path.split("-")
        if(temp(3) == "sports")
          thirdPath = "home_video"
        else if(temp.length >= 5 && temp(4) == "sports")
          thirdPath = "home_video"
        else
          thirdPath = x.group(2)
      case None => null
    }
    (year,month,date,week,subSection,thirdPath)
  }
}
