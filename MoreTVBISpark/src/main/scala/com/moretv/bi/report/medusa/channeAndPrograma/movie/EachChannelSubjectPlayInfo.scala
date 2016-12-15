package com.moretv.bi.report.medusa.channeAndPrograma.movie

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
 * Created by xiajun on 2016/5/16.
 * 统计不同频道的专题播放量，用于展示在各个频道的专题播放趋势图以及内容评估的专题趋势图
 */
object EachChannelSubjectPlayInfo extends BaseClass{
  private val regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu)([0-9]+)""".r

  def main(args: Array[String]): Unit = {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(EachChannelSubjectPlayInfo,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger/"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          val playviewInput = s"$medusaDir/$date/playview/"

          sqlContext.read.parquet(playviewInput).registerTempTable("log_data")


          sqlContext.read.parquet(playviewInput).select("userId","launcherAreaFromPath","launcherAccessLocationFromPath",
            "pageDetailInfoFromPath","pathIdentificationFromPath","path","pathPropertyFromPath","flag","event")
            .registerTempTable("log_data")

          val rdd = sqlContext.sql("select userId,launcherAreaFromPath,launcherAccessLocationFromPath," +
            "pageDetailInfoFromPath,pathIdentificationFromPath,path,pathPropertyFromPath,flag from log_data where event in ('startplay'," +
            "'playview')").repartition(20)

          val formattedRdd = rdd.map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e
            .getString(4),e.getString(5),e.getString(6),e.getString(7))).persist(StorageLevel.MEMORY_AND_DISK)

          val medusaInfoRdd=formattedRdd.filter(_._8=="medusa").filter(_._7=="subject").map(e=>{
            val subjectCode = MedusaSubjectNameCodeUtil.getSubjectCode(e._5)
            if(subjectCode!=" "){
              (getSubjectTypeFromSubjectCode(subjectCode,"moretv"),e._1)
            }else{
              (getSubjectTypeFromSubjectCode(e._5,e._8),e._1)
            }
          })

          val moretvInfoRdd = formattedRdd.filter(_._8=="moretv").map(e=>(e._1,e._6)).flatMap(e=>
            (SubjectUtils.getSubjectCodeAndPathWithId(e._2,e._1))).map(e=>(e._1._1,e._2)).filter(_._1!=null).map(e=>
            (getSubjectTypeFromSubjectCode(e._1,"moretv"),e._2))

          val mergerInfoRdd = medusaInfoRdd union moretvInfoRdd

          val eachTypeSubjectPlayNumMap=mergerInfoRdd.map(x => (x._1,1l)).reduceByKey(_ + _).collectAsMap()
          val eachTypeSubjectPlayUserMap=mergerInfoRdd.distinct().map(x => (x._1,1l)).reduceByKey(_ + _).collectAsMap()
          val mergerRdd=eachTypeSubjectPlayNumMap.map(e=>(e._1,e._2,eachTypeSubjectPlayUserMap(e._1)))

          if(p.deleteOld){
            val deleteSql="delete from medusa_channel_subject_play_info where day=?"
            util.delete(deleteSql,insertDate)
          }
          val sqlInsert = "insert into medusa_channel_subject_play_info(day,channel_name,play_num,play_user) values (?,?,?,?)"

          mergerRdd.foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1,new JLong(e._2),new JLong(e._3))
          })

          formattedRdd.unpersist()
        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

  def getSubjectTypeFromSubjectCode(subjectInfo:String,flag:String):String={
    if(subjectInfo!=null){
      if(flag=="moretv"){
        regex findFirstMatchIn subjectInfo match {
        case Some(m) => fromEngToChinese(m.group(1))
        case None => null
        }
      }else if(flag=="medusa"){
        val subjectCode=CodeToNameUtils.getSubjectCodeByName(subjectInfo)
        regex findFirstMatchIn subjectCode match {
          case Some(m) => fromEngToChinese(m.group(1))
          case None => null
        }
      }else null
    }else null

  }

  def fromEngToChinese(str:String):String={
    str match {
      case "movie"=>"电影"
      case "tv"=>"电视"
      case "hot"=>"资讯短片"
      case "kids"=>"少儿"
      case "zongyi"=>"综艺"
      case "comic"=>"动漫"
      case "jilu"=>"纪实"
      case "sports"=>"体育"
      case "xiqu"=>"戏曲"
    }
  }
}
