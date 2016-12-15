package com.moretv.bi.report.medusa.channeAndPrograma.mv.af310

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/28/16.
  */

/**
  * 统计音乐首页各的点击量
  * 数据源: positionaccess
  * 特征提取: accessArea, locationIndex, userId, belongTo
  * 过滤条件: mv , 首页各区域
  * 统计: pv, uv
  * 输出: tbl:[mv_position_click_stat](day, channel, access_area, location_index, pv, uv)
  */
object MVPositionClickStat extends BaseClass {

  private val dataSource = "positionaccess"

  private val tableName = "channel_position_click_stat"

  private val insertSql = s"insert into $tableName(day,channel,access_area,location_index,pv,uv)values(?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]) {

    ModuleClass.executor(MVPositionClickStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        // util & init
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val startDate = p.startDate

        val cal = Calendar.getInstance

        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        //
        (0 until p.numOfDays).foreach(w => {

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)

          cal.add(Calendar.DAY_OF_MONTH, -1)

          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path

          val loadPath = s"/log/medusa/parquet/$loadDate/$dataSource"
          println(loadPath)

          //df

          val df = sqlContext.read.parquet(loadPath)
            .select("belongTo", "accessArea", "locationIndex", "userId")
            .filter("accessArea is not null and locationIndex is not null")
            .filter("belongTo ='mv'")

          //rdd((channel, accessArea, locationIndex), userId)

          val rdd = df.map(
            e => (
              getAccessName(e.getString(0), e.getString(1), e.getString(2)), e.getString(3)
              )
          )
            .filter(_._1 != null)
            .cache

          //aggregate

          val uvMap = rdd.distinct.countByKey

          val pvMap = rdd.countByKey

          //handle with table


          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          uvMap.foreach(w => {

            val key = w._1

            val uv = w._2

            val pv = pvMap.get(key) match {
              case Some(p) => p
              case None => 0
            }

            util.insert(insertSql, sqlDate, w._1._1, w._1._2, w._1._3, new JLong(pv), new JLong(uv))
          })

        })
      }

      case None => {
        throw new Exception("MVPositionClickStat fails")
      }
    }
  }

  def getAccessName(channel: String, acceessArea: String, pos: String): (String, String, String) = {

    acceessArea match {

      case "mineHomePage" if pos == "1" => (channel, "我的", pos)

      case "mvRecommendHomePage"
        if (pos == "2" || pos == "3" || pos == "4" || pos == "5" || pos == "6" ||
          pos == "7" || pos == "8" || pos == "9" || pos == "10" || pos == "11" ||
          pos == "12" || pos == "13" || pos == "14")
      => (channel, "推荐", pos)

      case "mvTopHomePage"
        if (pos == "15" || pos == "16" || pos == "17" || pos == "18")
      => (channel, "榜单", pos)

      case "mvCategoryHomePage"
        if (pos == "19" || pos == "20" || pos == "21")
      => (channel, "分类", pos)

      case "function"
        if (pos == "1" || pos == "2" || pos == "3" || pos == "4" || pos == "5")
      => (channel, "入口", pos)

      case _ => null
    }
  }


}
