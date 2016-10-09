package com.moretv.bi.report.medusa.channeAndPrograma.mv

import java.util.Calendar
import java.lang.{Long => JLong}
import java.lang.{Double => JDouble}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 9/21/16.
  */

/**
  * MV 下的 精选集统计 (指定路径下)
  * 数据源: play (注 medusa3.1.0 有新增字段)
  * 提取特征: omnibusSid, omnibusName, userId, pathMain,duration
  * 统计: pv, uv ,mean_duration
  */
object MVOminibusPvUv extends BaseClass{

  private val tableName ="mv_ominibus_pv_uv_duration"

  private val filterPathRegex =("(mv\\*mvRecommendHomePage|mv\\*function\\*site_mvsubject|mv\\*mvCategoryHomePage\\*site" +
    "|home\\*recommendation|mv-mv-search|mv\\*mineHomePage\\*site_collect)").r

  def main(args: Array[String]) {

    ModuleClass.executor(MVOminibusPvUv, args)

  }


  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        // init & util
        val util = new DBOperationUtils("medusa")
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w=>{
          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          val loadPath = s"/log/medusa/parquet/$loadDate/play"

          //check schema
          if(sqlContext.read.parquet(loadPath).schema.simpleString contains("omnibusSid")){

            //df
            val df = sqlContext.read.parquet(loadPath).select("pathMain","omnibusSid","omnibusName","userId","event","duration")
              .filter("event='userexit'").filter("omnibusSid is not null").filter("omnibusName is not null").cache

            //rdd
            val pvuvRdd = df.map(e=>((filterEntrance(e.getString(0)),e.getString(1),e.getString(2)),e.getString(3)))
              .filter(_._1._1 != null).cache

            val durationRdd = df.map(e=>((filterEntrance(e.getString(0)),e.getString(1),e.getString(2)),e.getLong(5)))
              .filter(_._1._1 != null).cache

            //aggregate
            val uvMap = pvuvRdd.distinct.countByKey
            val pvMap = pvuvRdd.countByKey

            val durationMap = durationRdd.reduceByKey(_+_).collectAsMap()

            //deal with table
            if(p.deleteOld){
              util.delete(s"delete from $tableName where day =?",sqlDate)
            }

            uvMap.foreach(w=>{
              val key = w._1
              val pv = pvMap.get(key) match {
                case Some(p) => p
                case None => 0
              }
              val meanDuration = durationMap.get(key) match {
                case Some(p) => p.toFloat / w._2
                case None => 0
              }

              util.insert(s"insert into $tableName(day,entrance,ominibus_sid,ominibus_name,uv,pv,duration)values(?,?,?,?,?,?,?)",
                sqlDate,w._1._1,w._1._2,w._1._3,new JLong(w._2), new JLong(pv), new JDouble(meanDuration))
            })

          }

        })
      }
      case None => {
        throw new Exception("MVOminibusPvUv fails for not enough params")
      }
    }
  }

  /**
    *
    * @param field 用pathMain来确定父级入口
    * @return entrance 字段
    */
  def filterEntrance(field:String):String ={
    filterPathRegex findFirstMatchIn field match {
      case Some(p) =>{
        p.group(1) match {
          case "mv*mvRecommendHomePage" => "音乐首页推荐"
          case "mv*function*site_mvsubject" => "音乐入口"
          case "mv*mvCategoryHomePage*site" => "音乐下的某分类"
          case "home*recommendation" => "launcher推荐"
          case "mv-search" => "音乐搜索"
          case "mv*mineHomePage*site_collect" => "音乐收藏"
          case _ => null
        }
      }
      case None => null
    }
  }

}
