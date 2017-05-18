package com.moretv.bi.live

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.live.carousel_switching._
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by QIZHEN on 2017/5/16.
  * 计算轮播频道预约人数、预约次数
  */
object carousel_subscribe extends BaseClass {
  /** 定义存储轮播频道预约数据的表 **/
  private val tableName = "carousel_subscribe"
  private val insertSql = s"insert into ${tableName}(day,user_num,subscribe_num) values (?,?,?)"
  private val deleteSql = s"delete from ${tableName} where day = ?"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        /** 连接数据库为medusa **/
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        /** 开始处理日期 **/
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        /** 循环处理得到每一日数据并入库 **/
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          /** 预约日志 **/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.SUBSCRIBE, date).
            registerTempTable("subscribe")

          /** 计算轮播频道点击ok键显示播放列表的人数、点击ok键显示播放列表的次数 **/
          val updateCnt = sqlContext.sql(
            s"""
               |select  count(distinct userId) as user_num,
               |        count(userId) as click_num
               |from subscribe
               |where event='live' and subscribeType='carousel' and triggerCondition='subscribe'
            """.stripMargin).map(e => (e.get(0), e.get(1)))

          if (p.deleteOld) util.delete(deleteSql, insertDate)

          updateCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate, e._1, e._2)
          })

        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
