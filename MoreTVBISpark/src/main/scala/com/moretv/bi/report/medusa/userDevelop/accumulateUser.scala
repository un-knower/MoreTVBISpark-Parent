package com.moretv.bi.report.medusa.userDevelop

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by 陈佳星 on 2016/9/7.
  */
object accumulateUser extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(accumulateUser,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val dbsnapshotDir = "/log/dbsnapshot/parquet"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=> {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, 0)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          val newUserInput = s"$dbsnapshotDir/$date/moretv_mtv_account"

          sqlContext.read.parquet(newUserInput).registerTempTable("log_data")

          val rdd = sqlContext.sql(s"select product_model, count(distinct user_id) as user_num from log_data " +
            s"where openTime<= '$insertDate 23:59:59' and product_model is not null group by product_model " +
            s"order by user_num desc").map(e => (e.getString(0), e.getLong(1)))

          val sqlInsert = "insert into medusa_accumulate_user_from_product_model(day,product_model,user_num) " +
            "values (?,?,?)"
          if (p.deleteOld) {
            val deleteSql = "delete from medusa_accumulate_user_from_product_model where day=?"
            util.delete(deleteSql, insertDate)
          }
          rdd.collect().foreach(e => {
            util.insert(sqlInsert, insertDate, e._1, new JLong(e._2))
          })
        })
      }

      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

}
