package com.moretv.bi.temp.annual

import java.lang.Math

import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by witnes on 1/6/17.
  */

/**
  *
  */
object RetentionSideStat extends BaseClass {


  def main(args: Array[String]) {

    ModuleClass.executor(RetentionSideStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val q = sqlContext
        import q.implicits._

        val halfYearUdf = udf((q: Int) => {
          q match {
            case 1 | 2 => 0
            case 3 | 4 => 1
          }
        })

        val newUserPath = "/log/dbsnapshot/parquet/20170101/moretv_mtv_account"

        val loadPath = "/log/moretvloginlog/parquet/{2016*,20170101}/loginlog"


        val newUserDf = sqlContext.read.parquet(newUserPath)
          .filter(to_date($"openTime").between("2016-01-01", "2016-08-31"))
          .select($"mac", to_date($"openTime").as("date"), month($"openTime").as("month"))
          .withColumn("startDate", add_months($"date", 1))
          .withColumn("endDate", add_months($"date", 4))
          .distinct
          .drop($"date")


        val baseDf = sqlContext.read.parquet(loadPath)
          .filter($"mac".isNotNull)
          .filter("date between '2016-01-01' and '2016-12-31'")
          .select($"date", $"mac", month($"date").as("month"))

        baseDf.as("b").join(newUserDf.as("n"),
          $"b.mac" === $"n.mac" && $"b.month" === $"n.month")

        baseDf
          .groupBy($"mac", $"month")
          .agg(max($"date").as("maxMDate"), min($"date").as("minMDate"))



        baseDf
          .groupBy($"mac", $"month")
          .agg(max($"date").as("maxMDate"), min($"date").as("minMDate"))
          .as("b").join(
          newUserDf.as("n"),
          $"b.mac" === $"n.mac" && $"b.month".between(month($"n.startDate"), month($"n.endDate"))
            && $"b.maxMDate" < $"n.endDate" && $"b.minMDate" >= $"n.startDate"
        )
          .groupBy($"n.month")
          .agg(countDistinct($"n.mac"))
          .show(10, false)


      }
      case None => {

      }
    }

  }

  /**
    * 上个月使用过电视猫的用户基础上,这个月依然使用  month in [1:11]
    * 1 -> 2
    * 2 -> 3
    * ....
    * 11 -> 12
    *
    * @param df :DataFrame["month","mac"]  ps: already be distinct
    */
  def monthRetention(df: DataFrame) = {

    val q = sqlContext
    import q.implicits._


    val lastMonthUsers = df.filter($"month" between(1, 11))

    val nextMonthUsers = df.filter($"month" between(2, 12))
      .withColumn("month", $"month" - 1)

    val lastAgg = lastMonthUsers
      .groupBy($"month")
      .agg(count($"mac").as("uv"))

    val lastNextJointAgg = lastMonthUsers.as("l")
      .join(nextMonthUsers.as("n"),
        $"l.month" === $"n.month" && $"l.mac" === $"n.mac"
      ).groupBy($"n.month")
      .agg(count($"n.mac").as("uv"))

    lastAgg.as("l").join(lastNextJointAgg.as("j"), $"l.month" === $"j.month")
      .select($"j.month", round($"j.uv" / $"l.uv", 3))
      .collect.map(e => {
      (e.getInt(0), e.getDouble(1))
    })

  }

  def threeMonthRetention(df: DataFrame) = {
    val q = sqlContext
    import q.implicits._

    val lastMonthUsers = df.filter($"month".between(1, 9))
    val nextMonthUsers = df.filter($"month".between(2, 10)).withColumn("month", $"month" - 1)
    val nextTwoMonthUsers = df.filter($"month".between(3, 11)).withColumn("month", $"month" - 2)
    val nextThreeMonthUsers = df.filter($"month".between(4, 12)).withColumn("month", $"month" - 3)

    lastMonthUsers.as("l").join(nextMonthUsers.as("n1"),
      $"l.month" === $"n1.month" && $"l.mac" === $"n1.mac")
      .select($"l.month", $"l.mac")
      .as("j1").join(nextTwoMonthUsers.as("n2"),
      $"j1.month" === $"n2.month" && $"j1.mac" === $"n2.mac")
      .select($"j1.month", $"j1.mac")
      .as("j2").join(nextThreeMonthUsers.as("n3"),
      $"j2.month" === $"n3.month" && $"j2.mac" === $"n3.mac")
      .select($"j2.month", $"j2.mac")
      .groupBy($"j2.month")
      .agg(count($"j2.mac"))
      .show(100, false)


  }

  /**
    * 上个季度使用过电视猫的用户基础上,这个季度依然使用 quarter in [1:3]
    * 1 -> 2
    * 2 -> 3
    * 3 -> 4
    *
    * @param df :DataFrame["quater","mac"]  ps: already be distinct
    */
  def quarterRetention(df: DataFrame) = {

    val q = sqlContext
    import q.implicits._

    val lastQuaterUsers = df.filter($"quarter" between(1, 3))

    val nextQuarterUsers = df.filter($"quarter" between(2, 4)) // remove the first quarter
      .withColumn("quarter", $"quarter" - 1)

    val lastAgg = lastQuaterUsers.groupBy($"quarter")
      .agg(count($"mac").as("uv"))

    val lastNextJointAgg = lastQuaterUsers.as("l")
      .join(nextQuarterUsers.as("n"),
        $"l.quarter" === $"n.quarter" && $"l.mac" === $"n.mac")
      .groupBy($"n.quarter")
      .agg(count($"n.mac").as("uv"))

    lastAgg.as("l").join(lastNextJointAgg.as("j"), $"l.quarter" === $"j.quarter")
      .select($"j.quarter", round($"j.uv" / $"l.uv", 3))
      .collect.map(e => {
      (e.getInt(0), e.getDouble(1))
    })

  }


  /**
    * 上半年使用过电视猫的用户基础上,下半年依然使用  halfYear in (0,1)
    *
    * @param df :DataFrame["halfYear","mac"]  ps: already be distinct
    *           0 -> 1
    */
  def halfYearRetention(df: DataFrame): Unit = {

    val q = sqlContext
    import q.implicits._

    val lastHalfYearUsers = df.filter($"halfYear" === 1)
      .withColumn("halfYear", $"halfYear" - 1)

    val nextHalfYearUsers = df.filter($"halfYear" === 0)

    val lastHalfYearAgg = lastHalfYearUsers.count

    println(lastHalfYearAgg)

    val lastNextHalfYearJointAgg = lastHalfYearUsers.as("l")
      .join(nextHalfYearUsers.as("n"), $"l.halfYear" === $"n.halfYear" && $"l.mac" === $"n.mac")
      .count

    println(lastNextHalfYearJointAgg)


  }

  /**
    * 上个月不活跃的用户但在这个月活跃的人数 /  这整个月活跃人数
    *
    * @param df :DataFrame["month","mac"]  ps: already be distinct
    */
  def userBackFlowRate(df: DataFrame) = {

    val q = sqlContext
    import q.implicits._

    val lastMonthUsers = df.filter($"month" between(1, 11))
      .withColumn("month", $"month" + 1)

    val nextMonthUsers = df.filter($"month" between(2, 12))

    //    val nextMonthAgg = nextMonthUsers.groupBy($"month")
    //      .agg(count($"mac").as("uv"))

    val lastNextJointAgg = lastMonthUsers.as("l")
      .join(nextMonthUsers.as("n"), $"l.month" === $"n.month" && $"l.mac" === $"n.mac")
      .groupBy($"n.month".as("month"))
      .agg(count($"n.mac").as("uv"))

    lastNextJointAgg.show(100, false)
    //
    //
    //    nextMonthAgg.as("n").join(lastNextJointAgg.as("j"), $"n.month" === $"j.month")
    //      .select(($"n.uv" - $"j.uv"))
    //      .show(100, false)
  }

  /**
    * input => 2016*
    * 这个月活跃的用户但在后两个月并不活跃的人数 /  这整个月活跃人数这个月活跃人数
    * [1:10] -> [2:11] , [3:12]
    */
  def userSleepRate(df: DataFrame) = {

    val q = sqlContext
    import q.implicits._

    val lastMonthUsers = df.filter($"month" between(1, 10))

    val nextOneMonthUsers = df.filter($"month" between(2, 11))
      .withColumn("month", $"month" - 1)

    val nextTwoMonthUsers = df.filter($"month" between(3, 12))
      .withColumn("month", $"month" - 2)

    //    val lastMonthAgg = lastMonthUsers.groupBy($"month")
    //      .agg(count($"mac").as("uv"))

    //lastMonthAgg.show(100, false)

    val lastNextJointAgg = nextOneMonthUsers.unionAll(nextTwoMonthUsers).distinct.as("n")
      .join(lastMonthUsers.as("l"), $"n.month" === $"l.month" && $"l.mac" === $"n.mac")
      .groupBy($"n.month")
      .agg(count($"n.mac").as("uv"))
      .show(100, false)

    //    lastNextJointAgg.as("j").join(lastMonthAgg.as("l"), $"j.month" === $"l.month")
    //      .select(round(($"l.uv" - $"j.uv") / $"l.uv", 3))
    //      .show(100, false)

  }

  /**
    * 这个月使用电视猫的用户基础上，往后连续三个月没有使用 month in [1:9]
    *
    */
  def userAttritionRate(df: DataFrame) = {

    val q = sqlContext
    import q.implicits._


    val lastMonthUsers = df.filter($"month" between(1, 9))

    val nextOneMonthUers = df.filter($"month" between(2, 10))
      .withColumn("month", $"month" - 1)

    val nextTwoMonthUsers = df.filter($"month" between(3, 11))
      .withColumn("month", $"month" - 2)

    val nextThreeMonthUsers = df.filter($"month" between(4, 12))
      .withColumn("month", $"month" - 3)

    val lastMonthAgg = lastMonthUsers.groupBy($"month")
      .agg(count($"mac").as("uv"))

    val lastNextJointAgg = nextOneMonthUers
      .unionAll(nextTwoMonthUsers)
      .unionAll(nextThreeMonthUsers)
      .distinct.as("n")
      .join(lastMonthUsers.as("l"), $"l.mac" === $"n.mac" && $"l.month" === $"n.month")
      .groupBy($"n.month")
      .agg(count($"n.mac").as("uv"))

    lastNextJointAgg.as("j").join(lastMonthAgg.as("l"), $"j.month" === $"l.month")
      .select(round((($"l.uv" - $"j.uv") / $"l.uv"), 3))
      .show(100, false)

  }


}







