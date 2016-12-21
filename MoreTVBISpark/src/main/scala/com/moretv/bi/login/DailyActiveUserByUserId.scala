package com.moretv.bi.login

import java.sql.DriverManager

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by Will on 2016/2/16.
  */

/**
  * 数据源 ： loginlog
  * 维度： 日期
  * 度量： 新增用户数， 活跃用户数， 累计用户数，登录次数， 登录人数
  */
object DailyActiveUserByUserId extends BaseClass {

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val s = sqlContext
        import s.implicits._
        val inputDate = p.startDate
        val inputPath = s"/log/moretvloginlog/parquet/$inputDate/loginlog"

        val userIdActiveRdd =
          sqlContext.read.load(inputPath)
            .select("userId")
            .persist(StorageLevel.MEMORY_AND_DISK)

        val loginNum = userIdActiveRdd.count()                          //启动次数
        val userNum = userIdActiveRdd.distinct().count()                //启动人数

        val db = DataIO.getMySqlOps("moretv_bi_mysql")
        val day = DateFormatUtils.toDateCN(inputDate, -1)
        if (p.deleteOld) {
          val sqlDelete = "delete from login_detail where day = ?"
          db.delete(sqlDelete, day)
        }

        val util = DataIO.getMySqlOps("moretv_tvservice_mysql")

        val (min, max) = util.select2Tuple2[Long, Long](s"SELECT MIN(id),MAX(id) FROM tvservice.mtv_account " +
          s"WHERE openTime BETWEEN '$day 00:00:00' AND '$day 23:59:59'").head

        val userIdNewRDD = new JdbcRDD(sc, () => {
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-nect=true",
            "bi", "mlw321@moretv")
        },
          s"SELECT user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) = '$day'",
          min,
          max,
          30,
          r => r.getString(1))
          .distinct().toDF("userId")
          .persist(StorageLevel.MEMORY_AND_DISK)

        util.destory()

        val newUserNum = userIdNewRDD.count()                                     //新增人数
        val activeNum = userIdActiveRdd.distinct().except(userIdNewRDD).count()   //活跃人数

        val year = day.substring(0, 4).toInt
        val month = day.substring(5, 7).toInt
        val dayBefore = DateFormatUtils.toDateCN(inputDate, -2)
        val totalUserNumBefore =
          db.select[Long]("select totaluser_num from login_detail where day = ?", dayBefore)(row => row.getLong(0)).head
        val totalUserNum = totalUserNumBefore + newUserNum

        val sqlInsert = "insert into login_detail(year,month,day,totaluser_num,login_num,user_num,new_num,active_num) values(?,?,?,?,?,?,?,?)"
        db.insert(sqlInsert, year, month, day, totalUserNum, loginNum, userNum, newUserNum, activeNum)
        db.destory()
        userIdActiveRdd.unpersist()
      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }
}
