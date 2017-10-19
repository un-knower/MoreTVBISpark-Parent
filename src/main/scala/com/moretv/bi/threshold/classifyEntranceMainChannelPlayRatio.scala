package com.moretv.bi.threshold

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by QIZHEN on 2017/5/11.
  * 统计3.1.3版本主要频道（电影、电视剧、少儿、资讯、动漫、综艺）分类入口播放比
  */
object classifyEntranceMainChannelPlayRatio extends BaseClass{

  private val tableName = "classifyEntrance_mainChannel_playRatio"
  private val insertSql = s"insert into ${tableName}(day,version,contentType,playUser_cnt,homepageClickUser_cnt,playRatio) values (?,?,?,?,?,?)"
  private val deleteSql = s"delete from ${tableName} where day = ?"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        /**连接数据库为medusa**/
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        /**开始处理日期**/
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        /**循环处理得到每一日数据并入库**/
        (0 until p.numOfDays).foreach( i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          /**点播日志**/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.PLAY, date).
            registerTempTable(LogTypes.PLAY)

          sqlContext.sql(
            s"""
               |select apkVersion,
               |case when pathMain like '%movie-movie%' then 'movie'
               |     when pathMain like '%tv-tv%' then 'tv'
               |     when pathMain like '%zongyi-zongyi%' then 'zongyi'
               |     when pathMain like '%comic-comic%' then 'comic'
               |     when pathMain like '%hot-hot%' then 'hot'
               |     when pathMain like '%kids-kids%' then 'kids' end as contentType,
               |     userId
               |from ${LogTypes.PLAY}
               |where pathMain like '%classification%' and event = 'startplay'
            """.stripMargin).toDF("apkVersion","contentType","userId").registerTempTable("play_log")

          /**首页点击日志**/
          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MEDUSA, LogTypes.HOMEACCESS, date).
            registerTempTable(LogTypes.HOMEACCESS)

          /**分类入口的各频道播放比**/
          val updateUserCnt = sqlContext.sql(
            s"""
               |select a.apkVersion,a.contentType,a.playUser_cnt,b.homepageClickUser_cnt,
               |       a.playUser_cnt/b.homepageClickUser_cnt as playRatio
               |from
               |(  select apkVersion,
               |          contentType,
               |          count(distinct userId) as playUser_cnt
               |   from play_log
               |   where contentType in('movie','tv','kids','hot','comic','zongyi')
               |         and apkVersion ='3.1.3'
               |   group by apkVersion,contentType)a
               |left outer join
               | (  select apkVersion,
               |           accessLocation,
               |           count(distinct userId) as homepageClickUser_cnt
               |    from ${LogTypes.HOMEACCESS}
               |    where accessArea='classification' and apkVersion ='3.1.3'
               |    group by apkVersion,accessLocation)b
               |on a.contentType=b.accessLocation and a.apkVersion = b.apkVersion
            """.stripMargin).map(e=>(e.get(0),e.get(1),e.get(2),e.get(3),e.get(4)))

          if(p.deleteOld) util.delete(deleteSql,insertDate)

          updateUserCnt.collect.foreach(e => {
            util.insert(insertSql, insertDate,e._1, e._2,e._3,e._4,e._5)
          })
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}
