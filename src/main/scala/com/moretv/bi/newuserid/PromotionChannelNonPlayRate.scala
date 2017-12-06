package com.moretv.bi.newuserid

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by 连凯 on 2017/8/23.
  * 某种终端型号无播放行为用户比例统计
  * tablename: medusa.product_non_play_rate
  *
  */
object PromotionChannelNonPlayRate extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val s = sqlContext
        import s.implicits._

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        cal.add(Calendar.DAY_OF_MONTH, -1)

        for (i <- 0 until p.numOfDays) {

          val snapshotDate = DateFormatUtils.readFormat.format(cal.getTime)

          val playDates = getNextTwoWeekDates(snapshotDate)
          val dayCN = DateFormatUtils.toDateCN(snapshotDate)
          val promotionChannelList = p.paramMap("promotionChannelList").split("@@").mkString("','")


          if(p.deleteOld){
            val deleteSql = "delete from promotion_channel_non_play_rate where day = ?"
            util.delete(deleteSql,dayCN)

          }

          val dbKey = DataBases.MORETV_MEDUSA_MYSQL
          val sqlMinMaxId = s"select min(id),max(id) from mtv_account_migration where openTime between '$dayCN 00:00:00' and '$dayCN 23:59:59'"
          val sqlData = s"select user_id,promotion_channel from mtv_account_migration where id >= ? and id <= ? and openTime between '$dayCN 00:00:00' and '$dayCN 23:59:59' and promotion_channel in ('$promotionChannelList')"
          MySqlOps.getJdbcRDD(sc,dbKey,sqlMinMaxId,sqlData,100,rs => (rs.getString(1),rs.getString(2))).toDF("userId","promotionChannel").registerTempTable("snapshot_data")

          val newUserMap = sqlContext.sql("select promotionChannel,count(0) from snapshot_data group by promotionChannel").
            collect().map(row => (row.getString(0),row.getLong(1))).toMap
          sqlContext.read.load(s"/log/medusa/parquet/$playDates/play").
            select("userId").distinct().registerTempTable("play_data")

          val playMap = sqlContext.sql("select t.promotionChannel,count(0) as num from (select distinct a.promotionChannel,a.userId from snapshot_data a join play_data b on a.userId = b.userId) t  group by t.promotionChannel").
            collect().map(row => (row.getString(0),row.getLong(1))).toMap

          val insertSql = "insert into promotion_channel_non_play_rate(day,promotion_channel,new_user,play_user) values(?,?,?,?)"
          newUserMap.foreach(t => {
            val pc = t._1
            val newUser = t._2
            val playUser = playMap.getOrElse(pc,0)
            util.insert(insertSql,dayCN,pc,newUser,playUser)
          })

          cal.add(Calendar.DAY_OF_MONTH, -1)
        }



        util.destory()
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }


  def getNextTwoWeekDates(enDateStr:String):String = {
    val enFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(enFormat.parse(enDateStr))
    val now = enFormat.format(Calendar.getInstance().getTime)
    val dateStrs = (0 until 14).map(i => {
      cal.add(Calendar.DAY_OF_MONTH,1)
      val date = enFormat.format(cal.getTime)
      if(date.compareTo(now) > 0) null else date
    }).filter(_!=null).mkString(",")
    s"{$dateStrs}"
  }
}
