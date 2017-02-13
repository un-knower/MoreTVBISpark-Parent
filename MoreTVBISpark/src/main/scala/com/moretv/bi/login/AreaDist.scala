package com.moretv.bi.login

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.LogTypes
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/06/10.
  * 统计各地区的新增用户、活跃用户、活跃次数、累计用户数
  *
  */
object AreaDist extends BaseClass {

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val s = sqlContext
        import s.implicits._
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        val yesterday = DateFormatUtils.enDateAdd(inputDate,-1)

        DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG)
          .select("ip", "userId")
          .map(e => (ProvinceUtil.getChinaProvince(IPUtils.getProvinceByIp(e.getString(0))), e.getString(1)))
          .toDF("ip", "userId")
          .registerTempTable("active_data")

        val activeRows = sqlContext.sql("select ip,count(distinct userId),count(userId) from active_data group by ip")
          .collectAsList()



        val totalMap = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,yesterday).
          filter(s"left(openTime,10) <= '$day' and ip is not null").
          select("ip","user_id")
          .distinct
          .map(row => {
            (ProvinceUtil.getChinaProvince(IPUtils.getProvinceByIp(row.getString(0))), row.getString(1))
          })
          .countByKey



        val newMap =  DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,yesterday).
          filter(s"left(openTime,10) = '$day' and ip is not null").
          select("ip","user_id")
          .distinct
          .map(row => {
            (ProvinceUtil.getChinaProvince(IPUtils.getProvinceByIp(row.getString(0))), row.getString(1))
          })
          .countByKey


        val db = DataIO.getMySqlOps("moretv_eagletv_mysql")
        if (p.deleteOld) {
          val sqlDelete = "delete from login_detail where day = ?"
          db.delete(sqlDelete, day)
        }

        val sqlInsert = "insert into login_detail(day,area,new_num,user_num,log_num,active_num,total_num) values(?,?,?,?,?,?,?)"
        val activeMap = activeRows.map(row => (row.getString(0), (row.getLong(1), row.getLong(2)))).toMap
        totalMap.foreach(x => {
          val province = x._1
          val totalNum = x._2
          val (userNum, logNum) = activeMap.getOrElse(province, (0l, 0l))
          val newNum = newMap.getOrElse(province, 0l)
          val activeNum = userNum - newNum
          db.insert(sqlInsert, day, province, newNum, userNum, logNum, activeNum, totalNum)
        })
        db.destory()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}

