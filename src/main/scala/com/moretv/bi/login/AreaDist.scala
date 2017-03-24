package com.moretv.bi.login

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.functions._

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
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        val yesterday = DateFormatUtils.enDateAdd(inputDate,-1)

        val webLocationDf = DataIO.getDataFrameOps.getDimensionDF(
          sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_WEB_LOCATION
        )//.registerTempTable("dim_web_location")

        val loginDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG)
          .select("ip", "userId")

        val getIpKey = udf((ip: String) => {
          try {
            val ipInfo = ip.split("\\.")
            if (ipInfo.length >= 3) {
              (((ipInfo(0).toLong * 256) + ipInfo(1).toLong) * 256 + ipInfo(2).toLong) * 256
            } else 0
          }catch {
            case ex : Exception => 0
          }
        })

        loginDf.as("a").join(
          webLocationDf.as("b"),
          getIpKey(loginDf("ip")) === webLocationDf("web_location_key") && isnull(webLocationDf("dim_invalid_time")),
          "leftouter")
          .selectExpr("case when b.country = '中国' then b.province else '国外及其他' end as ip", "a.userId")
          .registerTempTable("active_data")

        val activeRows = sqlContext.sql("select ip,count(distinct userId),count(userId) from active_data group by ip")
          .collectAsList()

        sqlContext.udf.register("leftSub",(x:String,y:Int) => {
          if(x != null) x.substring(0,10) else x
        })

        //TODO 对用用户的分析，应该用用户主题的事实表来分析
        val totalDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,yesterday).
          filter(s"leftSub(openTime,10) <= '$day' and ip is not null").
          select("ip","user_id")
          .distinct

        val totalMap = totalDf.as("a").join(
          webLocationDf.as("b"),
          getIpKey(totalDf("ip")) === webLocationDf("web_location_key") && isnull(webLocationDf("dim_invalid_time")),
          "leftouter")
          .selectExpr("case when b.country = '中国' then b.province else '国外及其他' end as ip", "a.user_id")
          .map(row => {
            (row.getString(0), row.getString(1))
          })
          .countByKey


        val newDf = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,yesterday).
          filter(s"leftSub(openTime,10) = '$day' and ip is not null").
          select("ip","user_id")
          .distinct

        val newMap =  newDf.as("a").join(
          webLocationDf.as("b"),
          getIpKey(newDf("ip")) === webLocationDf("web_location_key") && isnull(webLocationDf("dim_invalid_time")),
          "leftouter")
          .selectExpr("case when b.country = '中国' then b.province else '国外及其他' end as ip", "a.user_id")
          .map(row => {
            (row.getString(0), row.getString(1))
          })
          .countByKey


        val db = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)
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

