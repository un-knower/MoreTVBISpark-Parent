package com.moretv.bi.temp

import java.util.Calendar
import java.lang.{Long => JLong}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.{AccountIDInfoUtils, DBOperationUtils}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import scala.runtime.AbstractFunction0

/**
  * Created by witnes on 10/14/16.
  */

/**
  * 月活 年龄 性别分布
  */
object UserAgeStat extends BaseClass {

  private val tableAgeName = "moretv_month_user_age"

  private val tableGenderName = "moretv_month_user"

  val genderFields = "month,gender,uv"

  val ageFields = "month,age,uv"

  val insertGenderSql = s"insert into($tableGenderName)(month,gender,uv)values(?,?,?)"

  val insertAgeSql = s"insert into($tableAgeName)(month,age,uv)values(?,?,?)"


  def main(args: Array[String]) {

    ModuleClass.executor(UserAgeStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    val util = new DBOperationUtils("medusa")

    val cal = Calendar.getInstance

    val loadPath3 = "/log/medusa/parquet/2016{0930}/*"

    val loadPath2 = "/mbi/parquet/*/2016{0930}/"

    val account2 = sqlContext.read.parquet(loadPath2)
      .select("accountId", "date")
      .filter("accountId is not null")

    val account3 = sqlContext.read.parquet(loadPath3)
      .select("accountId", "date")
      .filter("accountId is not null")

    //(accountId, age, gender)
    val rdd2 = account2.distinct.map(e => (e.getInt(0).toString,
      AccountIDInfoUtils.getAgeFromAccount(e.getInt(0).toString),
      AccountIDInfoUtils.getGenderFromAccount(e.getInt(0).toString)
      )
    )

    val rdd3 = account3.unionAll(account3)
      .distinct.map(e => (e.getLong(0).toString,
      AccountIDInfoUtils.getAgeFromAccount(e.getLong(0).toString),
      AccountIDInfoUtils.getGenderFromAccount(e.getLong(0).toString)))

    val rdd = rdd2.union(rdd3).distinct.cache()
    println(rdd.count())

//    val ageMap = rdd.map(e => (e._2, e._1)).filter(_._1 != null).countByKey
//
//    val genderMap = rdd.map(e => (e._3, e._1)).filter(_._1 != null).countByKey
//
//    ageMap.foreach(w => {
//
//      util.insert(insertAgeSql, "2016-09-13~2016-10-13", w._1, new JLong(w._2))
//    })
//
//    genderMap.foreach(w => {
//      util.insert(insertGenderSql, "2016-09-13~2016-10-13", w._1, new JLong(w._2))
//    })

  }

}


//class DbConnection extends AbstractFunction0[con]