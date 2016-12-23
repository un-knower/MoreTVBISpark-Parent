package com.moretv.bi.login

import java.sql.DriverManager

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.account.AccountAccess._
import com.moretv.bi.account.TotalUsersByAccount._
import com.moretv.bi.constant.Tables
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.rdd.JdbcRDD

import scala.collection.JavaConversions._

/**
  * Created by Will on 2016/06/10.
  * 统计各地区的新增用户、活跃用户、活跃次数、累计用户数
  *
  */
object AreaDist extends BaseClass {

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(AreaDist, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val s = sqlContext
        import s.implicits._
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate, -1)

        val logRdd = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.LOGINLOG)
          .select("ip", "userId")
          .map(e => (ProvinceUtil.getChinaProvince(IPUtils.getProvinceByIp(e.getString(0))), e.getString(1)))
          .toDF("ip", "userId")
          .registerTempTable("active_data")

        val activeRows = sqlContext.sql("select ip,count(distinct userId),count(userId) from active_data group by ip")
          .collectAsList()


        val db = DataIO.getMySqlOps("moretv_bi_mysql")
        val url = db.prop.getProperty("url")
        val driver = db.prop.getProperty("driver")
        val user = db.prop.getProperty("user")
        val password = db.prop.getProperty("password")

        val sqlInfo = s"SELECT ip,user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) <= '$day' and ip is not null"

        val (min, max) = db.queryMaxMinID(Tables.MTV_ACCOUNT, "id")

        val totalMap = MySqlOps.getJdbcRDD(sc, sqlInfo, Tables.USERIDUSINGACCOUNT,
          r => (r.getString(1), r.getString(2)), driver, url, user, password, (min, max), 300)
          .distinct
          .map(t => {
            (ProvinceUtil.getChinaProvince(IPUtils.getProvinceByIp(t._1)), t._2)
          })
          .countByKey


        val sqlInfo1 = s"SELECT ip,user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$day' and ip is not null"

        val newMap = MySqlOps.getJdbcRDD(sc, sqlInfo1, Tables.USERIDUSINGACCOUNT,
          r => (r.getString(1), r.getString(2)), driver, url, user, password, (min, max), 300)
          .distinct
          .map(t => {
            (ProvinceUtil.getChinaProvince(IPUtils.getProvinceByIp(t._1)), t._2)
          })
          .countByKey


        val db1 = DataIO.getMySqlOps("moretv_eagletv_mysql")
        if (p.deleteOld) {
          val sqlDelete = "delete from login_detail where day = ?"
          db1.delete(sqlDelete, day)
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

