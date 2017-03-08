package com.moretv.bi.metis

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.storage.StorageLevel

/**
 * Created by zhangyu on 2016/8/11.
 * 统计不同入口下进入文章详情页的人数次数
 * 根据统计需求,需要对日志中定义的入口entrance做重新映射
 * table_name: arcitle_detail_click_based_entrance
 * (id,day,entrance,user_num,access_num)
 */
object ArticleDetailClickBasedEntrance extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)

        val calendar = Calendar.getInstance()
        val startDay = p.startDate
        calendar.setTime(DateFormatUtils.readFormat.parse(startDay))

        (0 until p.numOfDays).foreach(x => {
          val logDay = DateFormatUtils.readFormat.format(calendar.getTime)
          val sqlDay = DateFormatUtils.toDateCN(logDay, -1)
          val logPath = s"/log/metis/parquet/$logDay/browse"

          sqlContext.read.load(logPath).
            select("userId", "contentType", "event", "entrance").
            registerTempTable("log_data")

          val sqlRdd = sqlContext.sql("select entrance, userId from log_data " +
            "where event = 'enter' and contentType = 'article'").
            map(row => {
              val entrance = fromEntranceToType(row.getString(0))
              val userId = row.getString(1)
              (entrance, userId)
            }).filter(_ != null).
            persist(StorageLevel.MEMORY_AND_DISK)

          val userNum = sqlRdd.distinct.countByKey()
          val accessNum = sqlRdd.countByKey()

          if (p.deleteOld) {
            val deleteSql = "delete from article_detail_click_based_entrance where day = ?"
            util.delete(deleteSql, sqlDay)
          }

          userNum.foreach(e => {
            val insertSql = "insert into article_detail_click_based_entrance(day,entrance,user_num,access_num) values(?,?,?,?)"
            util.insert(insertSql, sqlDay, e._1, e._2, accessNum(e._1))
          })

          calendar.add(Calendar.DAY_OF_MONTH, -1)
        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

  //重新映射对应入口名称
  def fromEntranceToType(str: String): String = {
    if (str != null && str.nonEmpty && str != "null") {
      str match {
        case "dailyPaper" => "dailyPaper"
        case "search" => "search"
        case "recommend" | "review" | "knowledge" | "adventures" | "character" | "interest" => "classification"
        case _ => str
      }
    }
    else "NULL"
  }
}
