package com.moretv.bi.account

import java.text.SimpleDateFormat
import java.util.Calendar
import java.lang.{Long => JLong}
import org.apache.spark.sql.functions._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util._
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.storage.StorageLevel

/**
  * Created by laishun on 15/10/9.
  */
object AccountAccess extends BaseClass with DateUtil {

  private val sql = "INSERT INTO account_access_users(year,month,day,accessCode,accessName,user_num,access_num) VALUES(?,?,?,?,?,?,?)"

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sq = sqlContext
        import sq.implicits._

        val getName: String => String = e => if (e == "access") "活跃用户" else "登录用户"
        val getNameUdf = udf(getName)

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)

        //delete old data
        if (p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from account_access_users where day = '$date'"
          util.delete(oldSql)
        }


        DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.MTVACCOUNT)
          .filter("event = 'access' or event = 'login'")
          .select(
            year($"date").as("year"),
            month($"date").as("month"),
            $"date".as("day"),
            getNameUdf($"event").as("userType"),
            $"userId")
          .groupBy("year", "month", "day", "userType")
          .agg(count("userId"), countDistinct("userId"))

          .collect.foreach(x => {
          util.insert(sql, new Integer(x.getInt(0)), new Integer(x.getInt(1)), x.getString(2), x.getString(3),
            new JLong(x.getLong(4)), new JLong(x.getLong(5)))
        })

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }
  }

}
