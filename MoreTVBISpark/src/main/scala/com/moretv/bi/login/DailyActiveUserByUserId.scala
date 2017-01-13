package com.moretv.bi.login

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
  * Created by Will on 2016/2/16.
  */

/**
  * 数据源 ： loginlog
  * 维度： 日期
  * 度量： 新增用户数， 活跃用户数， 累计用户数，登录次数， 登录人数
  */
object DailyActiveUserByUserId extends BaseClass {

  private val cnFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val readFormat = new SimpleDateFormat("yyyyMMdd")

  private val tableName = "login_detail"

  private val fields = "year,month,day,totaluser_num,login_num,user_num,new_num,active_num"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName($fields) where day = ?"


  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val s = sqlContext
        import s.implicits._

        val util = DataIO.getMySqlOps("moretv_bi_mysql")

        val cal = Calendar.getInstance

        (0 until p.numOfDays).foreach(w => {

          cal.setTime(readFormat.parse(p.startDate))
          val loadDate = readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_YEAR, -1)
          val loadDate1 = readFormat.format(cal.getTime)
          val sqlDate = cnFormat.format(cal.getTime)

          val loginUserDb =
            DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG, loadDate)
              .select("mac")

          val userInfoDb = DataIO.getDataFrameOps
            .getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, loadDate1)
            .select(to_date($"openTime").as("date"), $"user_id")

          val newUserInfo = userInfoDb.filter($"date" === sqlDate).select($"user_id")

          val totalUserInfo = userInfoDb.filter($"date" <= sqlDate).select($"user_id")


          val loginNum = loginUserDb.count //访问登录接口次数

          val userLoginNum = loginUserDb.distinct.count //访问登录接口人数

          val newUserNum = newUserInfo.count //新增人数

          val activeNum = userLoginNum - newUserNum //活跃人数

          val totalUserNum = totalUserInfo.count

          val year = cal.get(Calendar.YEAR)
          val month = cal.get(Calendar.MONTH) + 1

          println(year, month, sqlDate, totalUserNum, loginNum, userLoginNum, newUserNum, activeNum)

          //          if (p.deleteOld) {
          //            util.delete(deleteSql, sqlDate)
          //          }
          //
          //          util.insert(insertSql,
          //            year, month, sqlDate, totalUserNum, loginNum, userLoginNum, newUserNum, activeNum
          //          )

        })

        util.destory()

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
