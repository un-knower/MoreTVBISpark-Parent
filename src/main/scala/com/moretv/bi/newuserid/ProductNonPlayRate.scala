package com.moretv.bi.newuserid

import java.text.SimpleDateFormat
import java.util.Calendar

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
object ProductNonPlayRate extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val snapshotDate = DateFormatUtils.enDateAdd(p.startDate,-1)
        val playDates = getNextTwoWeekDates(snapshotDate)
        val dayCN = DateFormatUtils.toDateCN(snapshotDate)
        val productModelList = p.paramMap("productModelList").split("@@").mkString("','")

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        if(p.deleteOld){
          val deleteSql = "delete from product_non_play_rate where day = ?"
          util.delete(deleteSql,dayCN)

        }

        DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,snapshotDate).
          select("user_id","product_model","openTime").
          filter(s"openTime between '$dayCN 00:00:00' and '$dayCN 23:59:59' and product_model in ('$productModelList')").
          distinct().
          registerTempTable("snapshot_data")

        val newUserMap = sqlContext.sql("select product_model,count(0) from snapshot_data group by product_model").
          collect().map(row => (row.getString(0),row.getLong(1))).toMap
        sqlContext.read.load(s"/log/medusa/parquet/$playDates/play").
          select("userId").distinct().registerTempTable("play_data")

        val playMap = sqlContext.sql("select t.product_model,count(0) as num from (select distinct a.product_model,a.user_id from snapshot_data a join play_data b on a.user_id = b.userId) t  group by t.product_model").
          collect().map(row => (row.getString(0),row.getLong(1))).toMap

        val insertSql = "insert into product_non_play_rate(day,product_model,new_user,play_user) values(?,?,?,?)"
        newUserMap.foreach(t => {
          val pm = t._1
          val newUser = t._2
          val playUser = playMap.getOrElse(pm,0)
          util.insert(insertSql,dayCN,pm,newUser,playUser)
        })

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
