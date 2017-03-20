package com.moretv.bi.login

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by Will on 2016/2/16.
  */

/**
  * 数据源 ： loginlog
  * 维度： 日期
  * 度量： 新增用户数， 活跃用户数， 累计用户数，登录次数， 登录人数
  */
object InsertIntoMtvUserOverview extends BaseClass {

  private val cnFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val readFormat = new SimpleDateFormat("yyyyMMdd")

  private val tableName = "login_detail"

  private val fields = "year,month,day,totaluser_num,login_num,user_num,new_num,active_num"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"


  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)

        val cal = Calendar.getInstance
        cal.setTime(readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(w => {

          val loadDate = readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_YEAR, -1)
          val sqlDate = cnFormat.format(cal.getTime)
          val dayBefore = DateFormatUtils.toDateCN(loadDate, -2)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, loadDate)
              .select("mac").registerTempTable("log_data")

          val sqlMinMaxId =
            s"SELECT min(id),max(id) FROM `mtv_account` WHERE openTime between '$sqlDate 00:00:00' and '$sqlDate 23:59:59'"

          val sqlData = s"SELECT mac FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$sqlDate'"

          val newUserNum =
            MySqlOps.getJdbcRDD(sc, DataBases.MORETV_TVSERVICE_MYSQL, sqlMinMaxId, sqlData, 50, rs => rs.getString(1))
              .distinct().count()

          val (loginNum,userNum) = sqlContext.sql("select count(mac),count(distinct mac) from log_data").collect().
            map(row => (row.getLong(0),row.getLong(1))).head
          val activeNum = userNum - newUserNum //活跃人数

          val totalUserNumBefore =
            util.selectOne("select totaluser_num from login_detail where day = ?", dayBefore).head.toString.toInt


          val totalUserNum = totalUserNumBefore + newUserNum
          val year = cal.get(Calendar.YEAR)
          val month = cal.get(Calendar.MONTH) + 1


          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }

          util.insert(insertSql,
            year, month, sqlDate, totalUserNum, loginNum, userNum, newUserNum, activeNum
          )

        })

        util.destory()

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
