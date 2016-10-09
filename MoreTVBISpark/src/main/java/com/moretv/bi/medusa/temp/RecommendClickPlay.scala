package com.moretv.bi.medusa.temp

import java.util.Calendar

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import java.lang.{Long => JLong}
/**
  * Created by witnes on 9/12/16.
  */

/**
  * 统计一段时间 首页推荐位的推荐5 homeacceess 的 pv,uv 和 play 的 pv,uv
  */
object RecommendClickPlay extends BaseClass{

  private val tableName = "medusa_recommend_click_play"

  def main(args: Array[String]):Unit = {

    ModuleClass.executor(RecommendClickPlay, args)
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
          val readHomeAccess = s"/log/medusa/parquet/$date/homeaccess"
          val readPlayPath = s"/log/medusa/parquet/$date/play"
          println(readHomeAccess, readPlayPath)
          //df
          sqlContext.read.parquet(readHomeAccess).registerTempTable("log1")
          sqlContext.read.parquet(readPlayPath).registerTempTable("log2")

          val dfClick =
            sqlContext.sql("select date, apkVersion, userId from log1 where accessArea='recommendation' and locationIndex=5")
                .filter(s"date='$date1'")
                .filter("apkVersion in ('3.0.8','3.0.9') " )

          val dfPlay =
             sqlContext.sql("select date, apkVersion, userId from log2 where pathMain like 'home*recommendation*5%' and event='startplay'")
               .filter(s"date='$date1'")
               .filter("apkVersion in ('3.0.8','3.0.9')" )
          //rdd
          val rddClick = dfClick.map(e => ((e.getString(0), e.getString(1)), e.getString(2))).cache
          val rddPlay = dfPlay.map(e=>((e.getString(0),e.getString(1)),e.getString(2))).cache

         val clickPv = rddClick.countByKey

         val clickUv = rddClick.distinct.countByKey

         val playPv = rddPlay.countByKey

         val playUv = rddPlay.distinct.countByKey

          if(p.deleteOld){
            util.delete(s"delete from $tableName where day = ? ", date1)
          }

        clickPv.foreach(i=>{

          val key = i._1

          val c_uv = clickUv.get(key) match {
            case Some(e) => e
            case _ => 0L
          }
          val c_pv = clickPv.get(key) match {
            case Some(e) => e
            case _ => 0L
          }
          val p_uv = playUv.get(key) match {
            case Some(e) => e
            case _ => 0L
          }
          val p_pv = playPv.get(key) match {
            case Some(e) => e
            case _ => 0L
          }
          println(key._1,key._2,c_pv,c_uv,p_pv,p_uv)

          try{

            util.insert(s"insert into $tableName (day,apkVersion,click_pv,click_uv,play_pv,play_uv) " +
              s"values (?,?,?,?,?,?)", key._1,key._2,new JLong(c_pv),new JLong(c_uv),
              new JLong(p_pv),new JLong(p_uv))

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
