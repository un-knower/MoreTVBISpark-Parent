package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by xiajun on 2016/5/16.
 *
 */
object QuerySubjectViewInfoTemp extends SparkSetting{
  private val historyCollect=Array("history","collect","account")
  private val re="thirdparty".r


  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        config.set("spark.executor.memory", "5g").
          set("spark.executor.cores", "5").
          set("spark.cores.max", "100")
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          val playviewInput = s"$medusaDir/$date/detail/"

          sqlContext.read.parquet(playviewInput).select("userId","launcherAreaFromPath","launcherAccessLocationFromPath",
            "pageDetailInfoFromPath","pathIdentificationFromPath","path","pathPropertyFromPath","flag","event").
            repartition(10).registerTempTable("log_data")

          val rdd = sqlContext.sql("select userId,launcherAreaFromPath,launcherAccessLocationFromPath," +
            "pageDetailInfoFromPath,pathIdentificationFromPath,path,pathPropertyFromPath,flag from log_data where " +
            "event='view'").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),e.getString(5),e.getString(6),e.getString(7))).persist(StorageLevel.MEMORY_AND_DISK)

          val mergerInfoRdd = rdd.map(x => {
            val flag = x._8
            if(flag == "medusa" && x._7 == "subject"){
              List((getMedusaFormattedInfo(x._2,x._3,x._4,x._5),x._1))
            }else if(flag == "moretv"){
              SubjectUtils.getSubjectCodeAndPathWithId(x._6,x._1).
                map(e=>((e._1._1,changeSourceNameToChinese(e._1._2)),e._2))
            }else null
          }).filter(_ != null).flatMap(x=>x).persist(StorageLevel.MEMORY_AND_DISK)
          val playNumMap=mergerInfoRdd.map(e=>(e._1,1)).reduceByKey(_+_).collectAsMap()
          val playUserMap=mergerInfoRdd.distinct().map(e=>(e._1,1)).reduceByKey(_+_).collectAsMap()
          val insertSql="insert into medusa_content_evaluate_each_subject_view_query_info(day,subjectCode,title,source," +
            "view_num,view_user) values (?,?,?,?,?,?)"
          if(p.deleteOld){
            val deleteSql="delete from medusa_content_evaluate_each_subject_view_query_info where day=?"
            util.delete(deleteSql,insertDate)
          }

          playNumMap.foreach(e=>{
            val (subjectCode,source) = e._1
            val viewNum = e._2
            val viewUser = playUserMap(e._1)
            util.insert(insertSql,insertDate,subjectCode,CodeToNameUtils.getSubjectNameBySid(subjectCode),source,new JLong(viewNum),
            new JLong(viewUser))
          })

          mergerInfoRdd.unpersist()


        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

  def getMedusaFormattedInfo(area:String,accessLocation:String,pageDetailInfo:String,subjectName:String)={
    val subjectCode=CodeToNameUtils.getSubjectCodeByName(subjectName)
    area match {
      case "recommendation"=>(subjectCode,"3.X首页推荐")
      case "my_tv"=>{
        if(historyCollect.contains(accessLocation)){
          (subjectCode,"3.X历史收藏")
        }else{
          (subjectCode,pageDetailInfo)
        }
      }
      case "classification"=>(subjectCode,pageDetailInfo)
      case _ => (subjectCode,"其他路径")
    }
  }

  def changeSourceNameToChinese(source:String)={
    re findFirstMatchIn source match {
      case Some(p)=>"第三方应用"
      case None=>{
        source match {
          case "hotrecommend"=>"2.X首页推荐"
          case "history"=>"2.X历史收藏"
          case _=>CodeToNameUtils.getThirdPathName(source)
        }
      }
    }
  }

}
