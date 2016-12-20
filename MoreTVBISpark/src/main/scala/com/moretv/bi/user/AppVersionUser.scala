package com.moretv.bi.user

import java.lang.{Long => JLong}
import java.sql.DriverManager

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.constant.Tables
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
  * 创建人：连凯
  * 创建时间：2016-05-03
  * 程序用途：统计各版本截止某天的总用户数
  * 数据来源：2-15 tvservice用户库
 */
object AppVersionUser extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(AppVersionUser,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        val util = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
        val url = util.prop.getProperty("url")
        val driver = util.prop.getProperty("driver")
        val user = util.prop.getProperty("user")
        val password = util.prop.getProperty("password")
        val sqlInfo = s"SELECT current_version,mac FROM `mtv_account` WHERE ID >= ? AND ID <= ? and openTime <= '$day 23:59:59'"
        val ids = util.selectOne(s"SELECT MIN(id),MAX(id) FROM tvservice.mtv_account WHERE openTime <= '$day 23:59:59'")
        val sqlRDD = MySqlOps.getJdbcRDD(sc,sqlInfo,Tables.MTV_ACCOUNT,
          r=>(r.getString(1),r.getString(2)),driver,url,user,password,
          (ids(0).toString.toLong,ids(1).toString.toLong),200).
          map(t => if(t._1 == null) ("null",t._2) else t).distinct()
        val userNumMap = sqlRDD.countByKey()

        sc.stop()

        if(p.deleteOld) {
          val sqlDelete = s"delete from bi.app_version_user where day = '$day'"
          util.delete(sqlDelete)
        }
        //插入数据库，插入日期，版本号，总用户数
        val sqlInsert = "INSERT INTO bi.app_version_user(day,app_version,user_num) VALUES(?,?,?)"
        userNumMap.foreach(e => {
          util.insert(sqlInsert,day,e._1,new JLong(e._2))
        })

      }
      case None => throw new RuntimeException("At least need param --startDate.")
    }

  }

}
