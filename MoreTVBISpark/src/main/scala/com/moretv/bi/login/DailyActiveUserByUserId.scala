//package com.moretv.bi.login
//
//import java.text.SimpleDateFormat
//import java.util.Calendar
//
//import com.moretv.bi.util._
//import cn.whaley.sdk.dataexchangeio.DataIO
//import com.moretv.bi.global.{DataBases, LogTypes}
//import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.sql.functions._
//
///**
//  * Created by Will on 2016/2/16.
//  */
//
///**
//  * 数据源 ： loginlog
//  * 维度： 日期
//  * 度量： 新增用户数， 活跃用户数， 累计用户数，登录次数， 登录人数
//  */
//object DailyActiveUserByUserId extends BaseClass {
//
//  private val cnFormat = new SimpleDateFormat("yyyy-MM-dd")
//  private val readFormat = new SimpleDateFormat("yyyyMMdd")
//
//  def main(args: Array[String]): Unit = {
//    ModuleClass.executor(this, args)
//  }
//
//  override def execute(args: Array[String]) {
//
//    ParamsParseUtil.parse(args) match {
//      case Some(p) => {
//        val s = sqlContext
//        import s.implicits._
//
//        val util = DataIO.getMySqlOps("moretv_bi_mysql")
//
//        val cal = Calendar.getInstance
//
//        cal.setTime(readFormat.parse(p.startDate))
//        cal.add(Calendar.DAY_OF_YEAR, -1)
//        val day = cnFormat.format(cal)
//
//        if (p.deleteOld) {
//          val sqlDelete = "delete from login_detail where day = ?"
//          util.delete(sqlDelete, day)
//        }
//
//        val loginUser =
//          DataIO.getDataFrameOps.getDF(sc, p.paramMap, LOGINLOG, LogTypes.LOGINLOG)
//            .select("userId", "mac").join(loginUser, $"" )
//
//        val loginNum = loginUser.select("mac").count //访问登录接口次数
//        val userNum = loginUser.select("mac").distinct.count //访问登录接口人数
//
//
//        val userInfoDb = DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT)
//          .select(to_date($"openTime").as("date"), $"user_id".as("userId"), $"mac")
//
//
//        val newUserInfo = userInfoDb.filter(to_date($"openTime") === day)
//
//        val dd = newUserInfo.groupBy("").agg()
//
//
//        val totalUserInfo = userInfoDb.filter(to_date($"openTime") <= day).select("userId")
//
//
//        val newUserNum = newUserInfo.select("userId").count //新增人数
//        val activeNum = userIdActiveRdd.distinct().except(userIdNewRDD).count() //活跃人数
//
//        val year = cal.get(Calendar.YEAR)
//        val month = cal.get(Calendar.MONTH) + 1
//
//
//        val sqlInsert = "insert into login_detail(year,month,day,totaluser_num,login_num,user_num,new_num,active_num) values(?,?,?,?,?,?,?,?)"
//        util.insert(sqlInsert, year, month, day, totalUserNum, loginNum, userNum, newUserNum, activeNum)
//        util.destory()
//      }
//      case None => {
//        throw new RuntimeException("At least need param --startDate.")
//      }
//    }
//  }
//}
