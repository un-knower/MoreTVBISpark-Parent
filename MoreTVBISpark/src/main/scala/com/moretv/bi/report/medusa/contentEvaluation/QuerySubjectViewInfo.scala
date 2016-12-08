package com.moretv.bi.report.medusa.contentEvaluation

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.{FilesInHDFS, MedusaSubjectNameCodeUtil}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by xiajun on 2016/5/16.
 * 统计不同路径下专题的浏览量
 */
object QuerySubjectViewInfo extends BaseClass {
  private val historyCollect = Array("history", "collect", "account")
  private val re = "thirdparty".r
  private val regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu)([0-9]+)""".r
  private val subjectInfoMap = CodeToNameUtils.getAllSubjectCode()
  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100").
      set("spark.default.parallelism", "40").
      set("spark.sql.shuffle.partitions", "400").
      set("spark.cores.max", "100")
    ModuleClass.executor(QuerySubjectViewInfo, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val logType = "detail"
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          val inputDir = s"${medusaDir}${date}/${logType}"
          sqlContext.read.parquet(inputDir).registerTempTable("log_data")

          val rdd = sqlContext.sql("select userId,launcherAreaFromPath," +
            "launcherAccessLocationFromPath,pageDetailInfoFromPath," +
            "pathIdentificationFromPath,path,pathPropertyFromPath,flag " +
            "from log_data where event='view' and userId is not null and " +
            "((flag='medusa' and pathPropertyFromPath='subject') or flag='moretv')").
            map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),
            e.getString(4),e.getString(5),e.getString(6),e.getString(7))).
            repartition(28).cache()
          val mergerInfoRdd = rdd.map(x => {
            val flag = x._8
            if(flag == "medusa"){
              List((getMedusaFormattedInfo2(x._2,x._3,x._4,x._5),x._1))
            }
            else if(flag == "moretv"){
              SubjectUtils.getSubjectCodeAndPathWithId(x._6,x._1).
                map(e=>((e._1._1,changeSourceNameToChinese(e._1._2)),e._2))
            }else null
          }).repartition(80).filter(_ != null).flatMap(x=>x).cache()
          val viewNumMap=mergerInfoRdd.map(e=>(e._1,1)).countByKey()
          val viewUserMap=mergerInfoRdd.distinct().map(e=>(e._1,1)).countByKey()
          val insertSql="insert into medusa_content_evaluate_each_subject_view_query_info(day,subjectCode,title,source," +
            "view_num,view_user) values (?,?,?,?,?,?)"
          if(p.deleteOld){
            val deleteSql="delete from medusa_content_evaluate_each_subject_view_query_info where day=?"
            util.delete(deleteSql,insertDate)
          }

          viewNumMap.foreach(e=>{
            val (subjectCode,source) = e._1
            val viewNum = e._2
            val viewUser = viewUserMap(e._1)
            util.insert(insertSql,insertDate,subjectCode,CodeToNameUtils.getSubjectNameBySid(subjectCode),source,new JLong(viewNum),
            new JLong(viewUser))
          })

          mergerInfoRdd.unpersist()


        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

//  def getMedusaFormattedInfo(area: String, accessLocation: String, pageDetailInfo: String, subjectName: String) = {
//    val subjectInfo = MedusaSubjectNameCodeUtil.getSubjectCode(subjectName)
//    val subjectCode = if (subjectInfo == " ") {
//      CodeToNameUtils.getSubjectCodeByName(subjectName)
//    } else subjectInfo
//    area match {
//      case "recommendation" => (subjectCode, "3.X首页推荐")
//      case "my_tv" => {
//        if (historyCollect.contains(accessLocation)) {
//          (subjectCode, "3.X历史收藏")
//        } else {
//          (subjectCode, pageDetailInfo)
//        }
//      }
//      case "classification" => (subjectCode, pageDetailInfo)
//      case _ => (subjectCode, "其他路径")
//    }
//  }

  def getMedusaFormattedInfo2(area: String, accessLocation: String,
                              pageDetailInfo: String, subjectName: String) = {
    val subjectCode = regex findFirstMatchIn subjectName match {
      case Some(p) => p.group(1)+p.group(2)
      case None => subjectInfoMap.getOrElse(subjectName,"null")
    }
    area match {
      case "recommendation" => (subjectCode, "3.X首页推荐")
      case "my_tv" => {
        if (historyCollect.contains(accessLocation)) {
          (subjectCode, "3.X历史收藏")
        } else {
          (subjectCode, pageDetailInfo)
        }
      }
      case "classification" => (subjectCode, pageDetailInfo)
      case _ => (subjectCode, "其他路径")
    }
  }

  def changeSourceNameToChinese(source: String) = {
    re findFirstMatchIn source match {
      case Some(p) => "第三方应用"
      case None => {
        source match {
          case "hotrecommend" => "2.X首页推荐"
          case "history" => "2.X历史收藏"
          case _ => CodeToNameUtils.getThirdPathName(source)
        }
      }
    }
  }

}


