package com.moretv.bi.report.medusa.contentEvaluation

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.MedusaSubjectNameCodeUtil
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by 夏俊 on 2016/5/16.
 * 统计各个专题不同路径下的播放量情况
 */
object QuerySubjectPlayInfo extends BaseClass{
  private val historyCollect=Array("history","collect","account")
  private val mvRe = "(mineHomePage|mvRecommendHomePage|mvTopHomePage|horizontal)".r
  private val re="thirdparty.+".r


  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

           DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MERGER,LogTypes.PLAYVIEW,date).select("userId","launcherAreaFromPath","launcherAccessLocationFromPath",
            "pageDetailInfoFromPath","pathIdentificationFromPath","path","pathPropertyFromPath","flag","event","pathMain").
            repartition(16).registerTempTable("log_data")

          val rdd = sqlContext.sql("select userId,launcherAreaFromPath,launcherAccessLocationFromPath," +
            "pageDetailInfoFromPath,pathIdentificationFromPath,path,pathPropertyFromPath,flag,pathMain " +
            "from log_data where event in ('startplay'," +
            "'playview')").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e
            .getString(4),e.getString(5),e.getString(6),e.getString(7),e.getString(8))).persist(StorageLevel.MEMORY_AND_DISK)
          val medusaInfoRdd=rdd.filter(_._8=="medusa").filter(_._7=="subject").map(e=>(e._1,e._2,e._3,e._4,e._5,e._9))
          val formattedMedusaRdd=medusaInfoRdd.map(e=>(getMedusaFormattedInfo(e._2,e._3,e._4,e._5,e._6),e._1)).filter(_._1
            ._1!=null).filter(_._1._2!=null)

          val moretvInfoRdd=rdd.filter(_._8=="moretv").map(e=>(e._1,e._6))
          val formattedMoretvRdd=moretvInfoRdd.flatMap(e=>(SubjectUtils.getSubjectCodeAndPathWithId(e._2,e._1)))
            .map(e=>((e._1._1,changeSourceNameToChinese(e._1._2)),e._2)).filter(_._1._1!=null).filter(_._1
            ._2!=null)

          val mergerInfoRdd=formattedMedusaRdd.union(formattedMoretvRdd).persist(StorageLevel.MEMORY_AND_DISK)
          val playNum=mergerInfoRdd.map(e=>(e._1,1)).reduceByKey(_+_)
          val playUser=mergerInfoRdd.distinct().map(e=>(e._1,1)).reduceByKey(_+_)

          val mergerPlayInfo=playNum join playUser

          val insertSql="insert into medusa_content_evaluate_each_subject_play_query_info(day,subjectCode,title,source," +
            "play_num,play_user) values (?,?,?,?,?,?)"
          if(p.deleteOld){
            val deleteSql="delete from medusa_content_evaluate_each_subject_play_query_info where day=?"
            util.delete(deleteSql,insertDate)
          }

          mergerPlayInfo.foreachPartition(partition=>{
            val util1 = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
            partition.foreach(e=>{
              try{
                util1.insert(insertSql,insertDate,e._1._1,CodeToNameUtils.getSubjectNameBySid(e._1._1),e._1._2,new JLong(e._2._1),
                  new JLong(e._2._2))
              }catch{
                case e:Exception=>{println("QuerySubjectPlayInfo: Insert into table existing error! The exception is: ")
                  e.printStackTrace()}
              }
            })
          })
          mergerInfoRdd.unpersist()
          rdd.unpersist()

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

  def getMedusaFormattedInfo(area:String,accessLocation:String,pageDetailInfo:String,subjectName:String,pathMain:String)={
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
      case "classification"=>if(accessLocation=="mv"){
        val entrance = mvRe findFirstMatchIn pathMain match {
          case Some(p) => {
            p.group(1) match {
              case "mineHomePage" => "音乐首页推荐"
              case "mvRecommendHomePage" => "音乐首页推荐"
              case "mvTopHomePage" => "音乐榜单"
              case "horizontal" => "音乐首页推荐"
              case _ => "其他路径"
            }
          }
          case None => "其他路径"
        }
        (subjectCode,entrance)
      }else (subjectCode,pageDetailInfo)
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
