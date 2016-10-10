package com.moretv.bi.report.medusa.tempStatistic

import java.lang.{Long => JLong,Double => JDouble}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.MedusaSubjectNameCodeUtil
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
 * Created by 夏俊 on 2016/5/16.
 * 统计各个专题不同路径下的播放量情况
 */
object EachSubjectPlayDistributionInfo extends BaseClass{
  private val historyCollect=Array("history","collect","account")
  private val re="thirdparty.+".r


  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(EachSubjectPlayDistributionInfo,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          val playviewInput = s"$medusaDir/$date/playview/"

          sqlContext.read.parquet(playviewInput).select("userId","launcherAreaFromPath","launcherAccessLocationFromPath",
            "pageDetailInfoFromPath","pathIdentificationFromPath","path","pathPropertyFromPath","flag","event").
            repartition(16).registerTempTable("log_data")

          val rdd = sqlContext.sql("select userId,launcherAreaFromPath,launcherAccessLocationFromPath," +
            "pageDetailInfoFromPath,pathIdentificationFromPath,path,pathPropertyFromPath,flag from " +
            "log_data where event in ('startplay','playview')").map(e=>(e.getString(0),e.getString(1),
            e.getString(2),e.getString(3),e.getString(4),e.getString(5),e.getString(6),e.getString(7))).
            persist(StorageLevel.MEMORY_AND_DISK)
          val medusaInfoRdd=rdd.filter(_._8=="medusa").filter(_._7=="subject").map(e=>(e._1,e._2,e._3,e._4,e._5))
          val formattedMedusaRdd=medusaInfoRdd.map(e=>(getMedusaFormattedInfo(e._2,e._3,e._4,e._5),e._1)).filter(_._1
            ._1!=null).filter(_._1._2!=null)

          val moretvInfoRdd=rdd.filter(_._8=="moretv").map(e=>(e._1,e._6))
          val formattedMoretvRdd=moretvInfoRdd.flatMap(e=>(SubjectUtils.getSubjectCodeAndPathWithId(e._2,e._1)))
            .map(e=>((e._1._1,changeSourceNameToChinese(e._1._2)),e._2)).filter(_._1._1!=null).filter(_._1
            ._2!=null)

          val mergerInfoRdd=formattedMedusaRdd.union(formattedMoretvRdd).persist(StorageLevel.MEMORY_AND_DISK)
          val playNum=mergerInfoRdd.map(e=>((e._1._1,e._1._2,e._2),1)).reduceByKey(_+_)
          val totalMap = playNum.map(e=>((e._1._1,e._1._2),e._1._3)).countByKey()
          val distributionMap = playNum.map(e=>((e._1._1,e._1._2,e._2),e._1._3)).countByKey()
          val insertSql="insert into tmp_subject_play_distribution_info(day,subject_code,title,source," +
            "play_num,total_num,frequency,frequency_ratio) values (?,?,?,?,?,?,?,?)"
          if(p.deleteOld){
            val deleteSql="delete from tmp_subject_play_distribution_info where day=?"
            util.delete(deleteSql,insertDate)
          }

          distributionMap.foreach(e=>{
              try{
                val key = (e._1._1,e._1._2)
                val totalNum = totalMap.get(key) match {
                  case Some(e) => e
                  case None => 0L
                }
                util.insert(insertSql,insertDate,e._1._1,CodeToNameUtils.getSubjectNameBySid(e._1._1),e._1._2,
                  new JLong(e._1._3),new JLong(totalNum),new JLong(e._2),new JDouble(e._2*1.0/totalNum))
              }catch{
                case e:Exception=>{println("EachSubjectPlayDistributionInfo: Insert into table existing error! The exception is: ")
                  e.printStackTrace()}
              }
            })


          mergerInfoRdd.unpersist()
          rdd.unpersist()

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

  def getMedusaFormattedInfo(area:String,accessLocation:String,pageDetailInfo:String,subjectName:String)={
    val subjectInfo = MedusaSubjectNameCodeUtil.getSubjectCode(subjectName)
    val subjectCode=if(subjectInfo==" ") {CodeToNameUtils.getSubjectCodeByName(subjectName)} else {subjectInfo}
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
