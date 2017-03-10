package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object SportPlayAndLiveTotalUserInfo extends BaseClass{

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

      (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)


          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.PLAY,date).registerTempTable("log")
          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.LIVE,date).registerTempTable("log1")
          val sportPlayUserRdd=sqlContext.sql("select distinct userId from log where contentType = 'sports'" +
            " and event ='startplay'").map(e=>e.getString(0))
          val sportLiveUserRdd=sqlContext.sql("select distinct userId from log1 where channelSid like '%sport%'").map(e=>e
            .getString(0))
          val totalUserRdd=sportLiveUserRdd.union(sportPlayUserRdd)
          val totalUserNum=totalUserRdd.distinct().count()

          if (p.deleteOld) {
            val oldSql = s"delete from medusa_channel_play_and_live_user_sport_info where day = '$insertDate'"
            util.delete(oldSql)
          }

          val sqlInsert = "insert into medusa_channel_play_and_live_user_sport_info(day,total_user) values (?,?)"

          util.insert(sqlInsert,insertDate,new JLong(totalUserNum))

        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
