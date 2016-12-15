package com.moretv.bi.report.medusa.contentEvaluation

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.report.medusa.util.{FilesInHDFS, MedusaSubjectNameCodeUtil}
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by xiajun on 2016/5/16.
 * 统计不同路径下专题的浏览量
 */
object QuerySubjectViewInfoTest extends BaseClass {
  private val historyCollect = Array("history", "collect", "account")
  private val re = "thirdparty".r

  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100").
      set("spark.default.parallelism", "40").
      set("spark.sql.shuffle.partitions", "400").
      set("spark.cores.max", "100")
    ModuleClass.executor(QuerySubjectViewInfoTest, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          //          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          val files = FilesInHDFS.getFileFromHDFS(s"/log/medusaAndMoretvMerger/${date}/detail").map(f=>f.getPath.getName)
          /*
          (0 until files.size-38).foreach(i=>{
            executeComp(files(i), date)
          })
          */

          /*
          for (fileName <- files) {
            //executeComp(fileName, date)
            executeComp("part", date)
          }
          */
          for (i <- 1 to 2) {
            executeComp(  date)
          }
          /*
          for (i <- 1 to 2) {
            executeComp("part", date)
          }
          */

          System.exit(-1)

          //          val playviewInput = s"$medusaDir/$date/detail/part-r-00000-47879f11-9c92-4b7c-94b9-091f508a5048.gz.parquet"
          //
          //          sqlContext.read.parquet(playviewInput).select("userId","launcherAreaFromPath","launcherAccessLocationFromPath",
          //            "pageDetailInfoFromPath","pathIdentificationFromPath","path","pathPropertyFromPath","flag","event").
          //            repartition(24).filter("userId is not null").
          //            filter("(flag='medusa' and pathPropertyFromPath='subject') or flag='moretv'").registerTempTable("log_data")
          //
          //          val rdd = sqlContext.sql("select userId,launcherAreaFromPath,launcherAccessLocationFromPath," +
          //            "pageDetailInfoFromPath,pathIdentificationFromPath,path,pathPropertyFromPath,flag from log_data where " +
          //            "event='view'").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),
          //            e.getString(5),e.getString(6),e.getString(7))).repartition(28).cache()
          //          val mergerInfoRdd = rdd.repartition(50).map(x => {
          //            val flag = x._8
          //            if(flag == "medusa" && x._7 == "subject"){
          //              List((getMedusaFormattedInfo(x._2,x._3,x._4,x._5),x._1))
          //            }
          //            else if(flag == "moretv"){
          //              SubjectUtils.getSubjectCodeAndPathWithId(x._6,x._1).
          //                map(e=>((e._1._1,changeSourceNameToChinese(e._1._2)),e._2))
          //            }else null
          //          }).repartition(16).filter(_ != null).flatMap(x=>x).cache()

          //          val medusaInfoRdd=rdd.filter(_._8=="medusa").map(e=>(e._1,e._2,e._3,e._4,e._5))
          //          val formattedMedusaRdd=medusaInfoRdd.map(e=>(getMedusaFormattedInfo(e._2,e._3,e._4,e._5),e._1))
          //
          //          val moretvInfoRdd=rdd.filter(_._8=="moretv").map(e=>(e._1,e._6))
          //          val formattedMoretvRdd=moretvInfoRdd.flatMap(e=>(SubjectUtils.getSubjectCodeAndPathWithId(e._2,e._1)))
          //            .map(e=>((e._1._1,changeSourceNameToChinese(e._1._2)),e._2))
          //          val mergerInfoRdd=formattedMedusaRdd.union(formattedMoretvRdd).cache()
          //          mergerInfoRdd.map(e=>(s"${e._1._1}")).collect()
          //            System.exit(-1)
          //          val playNumMap=mergerInfoRdd.map(e=>(e._1,1))
          //            .reduceByKey(_+_).repartition(16).
          //                          collectAsMap()
          //          val playUserMap=mergerInfoRdd.distinct().map(e=>(e._1,1)).reduceByKey(_+_).repartition(16).
          //                          collectAsMap()
          //          val playNumMap=mergerInfoRdd.map(e=>(e._1,1)).repartition(16).
          //            collectAsMap()
          //          val playUserMap=mergerInfoRdd.distinct().map(e=>(e._1,1)).repartition(16).
          //            collectAsMap()
          //          val insertSql="insert into medusa_content_evaluate_each_subject_view_query_info(day,subjectCode,title,source," +
          //            "view_num,view_user) values (?,?,?,?,?,?)"
          //          if(p.deleteOld){
          //            val deleteSql="delete from medusa_content_evaluate_each_subject_view_query_info where day=?"
          //            util.delete(deleteSql,insertDate)
          //          }

          //          playNumMap.foreach(e=>{
          //            val (subjectCode,source) = e._1
          //            val viewNum = e._2
          //            val viewUser = playUserMap(e._1)
          //            util.insert(insertSql,insertDate,subjectCode,CodeToNameUtils.getSubjectNameBySid(subjectCode),source,new JLong(viewNum),
          //            new JLong(viewUser))
          //          })

          //          mergerInfoRdd.unpersist()


        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

  def getMedusaFormattedInfo(area: String, accessLocation: String, pageDetailInfo: String, subjectName: String) = {
    val subjectInfo = MedusaSubjectNameCodeUtil.getSubjectCode(subjectName)
    val subjectCode = if (subjectInfo == " ") {
      CodeToNameUtils.getSubjectCodeByName(subjectName)
    } else subjectInfo
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

  def executeComp( date: String) = {
    val medusaDir = "/log/medusaAndMoretvMerger/"
    //if (fileName.contains("part")) {
      //val playviewInput = s"$medusaDir/$date/detail/$fileName"
      val playviewInput = s"$medusaDir$date/detail/part-r-00040-e4c53c11-2b32-49f3-98a9-fdd033b8e3d4.gz.parquet"
      println(s"process file 12: ${playviewInput}")
      sqlContext.read.parquet(playviewInput).select("userId", "launcherAreaFromPath", "launcherAccessLocationFromPath",
        "pageDetailInfoFromPath", "pathIdentificationFromPath", "path", "pathPropertyFromPath", "flag", "event").
        repartition(24).filter("userId is not null").
        filter("(flag='medusa' and pathPropertyFromPath='subject') or flag='moretv'").registerTempTable("log_data")

      val rdd = sqlContext.sql("select userId,launcherAreaFromPath,launcherAccessLocationFromPath," +
        "pageDetailInfoFromPath,pathIdentificationFromPath,path,pathPropertyFromPath,flag from log_data where " +
        "event='view'").map(e => (e.getString(0), e.getString(1), e.getString(2), e.getString(3), e.getString(4),
        e.getString(5), e.getString(6), e.getString(7))).repartition(28).cache()

    val moretvInfoRdd = rdd.filter(_._8 == "moretv").map(e => (e._1, e._6))
    println(System.currentTimeMillis())
    val formattedMoretvRdd = moretvInfoRdd.flatMap(e => (SubjectUtils.getSubjectCodeAndPathWithId(e._2, e._1)))
      .map(e => ((e._1._1, changeSourceNameToChinese(e._1._2)), e._2))
    println(System.currentTimeMillis())
    println(formattedMoretvRdd.count)
      val medusaInfoRdd = rdd.filter(_._8 == "medusa").map(e => (e._1, e._2, e._3, e._4, e._5))
    println(System.currentTimeMillis())
      val formattedMedusaRdd = medusaInfoRdd.map(e => (getMedusaFormattedInfo(e._2, e._3, e._4, e._5), e._1))
    println(System.currentTimeMillis())
    formattedMedusaRdd.count()




      val mergerInfoRdd = formattedMedusaRdd.union(formattedMoretvRdd).cache()
      mergerInfoRdd.map(e => (s"${e._1._1}")).collect()
    //}
  }

}
