package com.moretv.bi.medusa.temp

import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import java.lang.{Long => JLong}
/**
  * Created by witnes on 9/12/16.
  */
object RecommendClickPlaySubject extends BaseClass{

  private val tableName = "medusa_recommend_play_subject"

  private val filterStr = "comic13,movie886,movie871"


  def main(args: Array[String]):Unit = {

    ModuleClass.executor(RecommendClickPlaySubject, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = new DBOperationUtils("medusa")
        //date
        val cal = Calendar.getInstance
        val startDate = p.startDate
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {
          //date
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val date1 = DateFormatUtils.cnFormat.format(cal.getTime)
          println(date)
          println(date1)
          //path
          val readPlayPath = s"/log/medusa/parquet/$date/play"

          //df
          sqlContext.read.parquet(readPlayPath).select("date","apkVersion","userId","datetime","pathMain","pathSpecial","event")
            .filter(s"date='$date1'").filter(s"datetime >='$date1 09:00:00'").filter("apkVersion in ('3.0.9','3.0.8')")
             .filter("pathMain like 'home*recommendation%'").filter("event='startplay'").registerTempTable("log1")

          val dfPlay1 =
            sqlContext.sql("select date, apkVersion, userId,'comic13' from log1 where pathSpecial like '%comic13'")

          val dfPlay2 =
            sqlContext.sql("select date, apkVersion, userId,'movie886' from log1 where pathSpecial like '%movie886'")

          val dfPlay3 =
            sqlContext.sql("select date, apkVersion, userId,'movie871' from log1 where pathSpecial like '%movie871'")


          //rdd

          val rddPlay1 = dfPlay1.map(e=>((e.getString(0),e.getString(1),e.getString(3)),e.getString(2))).cache
          val rddPlay2 = dfPlay2.map(e=>((e.getString(0),e.getString(1),e.getString(3)),e.getString(2))).cache
          val rddPlay3 = dfPlay3.map(e=>((e.getString(0),e.getString(1),e.getString(3)),e.getString(2))).cache

          val playPv1 = rddPlay1.countByKey
          val playUv1 = rddPlay1.distinct.countByKey

          val playPv2 = rddPlay2.countByKey
          val playUv2 = rddPlay2.distinct.countByKey

          val playPv3 = rddPlay3.countByKey
          val playUv3 = rddPlay3.distinct.countByKey

          if(p.deleteOld){
            util.delete(s"delete from $tableName where day = ? ", date1)
          }

          playPv1.foreach(i=>{

            val key = i._1
            val p_uv = playUv1.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            val p_pv = playPv1.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            println(key._1,key._2,p_pv,p_uv)

            try{

              util.insert(s"insert into $tableName (day,subjectCode,apkVersion,play_pv,play_uv) " +
                s"values (?,?,?,?,?)", key._1,key._3,key._2, new JLong(p_pv),new JLong(p_uv))

            }catch {
              case ex:Exception => {
                println(ex)
                throw new RuntimeException("sql fail")
              }
            }
          })

          playPv2.foreach(i=>{

            val key = i._1
            val p_uv = playUv2.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            val p_pv = playPv2.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            println(key._1,key._2,p_pv,p_uv)

            try{

              util.insert(s"insert into $tableName (day,subjectCode,apkVersion,play_pv,play_uv) " +
                s"values (?,?,?,?,?)", key._1,key._3,key._2, new JLong(p_pv),new JLong(p_uv))

            }catch {
              case ex:Exception => {
                println(ex)
                throw new RuntimeException("sql fail")
              }
            }
          })

          playPv3.foreach(i=>{

            val key = i._1
            val p_uv = playUv3.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            val p_pv = playPv3.get(key) match {
              case Some(e) => e
              case _ => 0L
            }
            println(key._1,key._2,p_pv,p_uv)

            try{

              util.insert(s"insert into $tableName (day,subjectCode,apkVersion,play_pv,play_uv) " +
                s"values (?,?,?,?,?)", key._1,key._3,key._2, new JLong(p_pv),new JLong(p_uv))

            }catch {
              case ex:Exception => {
                println(ex)
                throw new RuntimeException("sql fail")
              }
            }
          })
        })

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }



  }


}
