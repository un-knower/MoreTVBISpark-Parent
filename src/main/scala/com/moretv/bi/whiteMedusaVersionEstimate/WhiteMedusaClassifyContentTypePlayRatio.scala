package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by QIZHEN on 2017/5/3.
  */
object WhiteMedusaClassifyContentTypePlayRatio extends BaseClass {
  private val tableName = "whiteMedusa_classify_contentType_playRatio"
  private val insertSql = s"insert into ${tableName}(day,version,contentType,playUser_cnt,homepageClickUser_cnt,playRatio) values (?,?,?,?,?,?)"
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

          /** 点播日志 **/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY, date).
            registerTempTable(LogTypes.PLAY)

          sqlContext.sql(
            s"""
               |select apkVersion,
               |case when pathMain like '%movie-movie%' then 'movie'
               |     when pathMain like '%tv-tv%' then 'tv'
               |     when pathMain like '%sports%' then 'sport'
               |     when pathMain like '%zongyi-zongyi%' then 'zongyi'
               |     when pathMain like '%comic-comic%' then 'comic'
               |     when pathMain like '%jilu-jilu%' then 'jilu'
               |     when pathMain like '%hot-hot%' then 'hot'
               |     when pathMain like '%kids-kids%' then 'kids' end as contentType,
               |     userId
               |from ${LogTypes.PLAY}
               |where pathMain like '%classification%'
            """.stripMargin).toDF("apkVersion", "contentType", "userId").registerTempTable("play_log")

          /** 首页点击日志 **/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.HOMEACCESS, date).
            registerTempTable(LogTypes.HOMEACCESS)

          /** 分类入口的各频道播放比 **/
          val updateUserCnt = sqlContext.sql(
            s"""
               |select a.version,a.contentType,a.playUser_cnt,b.homepageClickUser_cnt,
               |       a.playUser_cnt/b.homepageClickUser_cnt as playRatio
               |from
               |(  select case when apkVersion='3.1.4' then 'new'
               |               when apkVersion<'3.1.4' then 'old' end as version,
               |          contentType,
               |          count(distinct userId) as playUser_cnt
               |   from play_log
               |   group by case when apkVersion='3.1.4' then 'new'
               |             when apkVersion<'3.1.4' then 'old' end,contentType)a
               |left outer join
               | (  select case when apkVersion='3.1.4' then 'new'
               |                when apkVersion<'3.1.4' then 'old' end as version,
               |           accessLocation,
               |           count(distinct userId) as homepageClickUser_cnt
               |    from ${LogTypes.HOMEACCESS}
               |    where accessArea='classification'
               |    group by case when apkVersion='3.1.4' then 'new'
               |             when apkVersion<'3.1.4' then 'old' end,accessLocation)b
               |on a.contentType=b.accessLocation and a.version = b.version
            """.stripMargin).map(e => (e.get(0), e.get(1), e.get(2), e.get(3), e.get(4)))

          if (p.deleteOld) util.delete(deleteSql, insertDate)

          updateUserCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate, e._1, e._2, e._3, e._4, e._5)
          })
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
