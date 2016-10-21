package com.moretv.bi.temp.user

import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 10/20/16.
  */
object PromotionActiveUser extends BaseClass {

  private val tableName = "promotion_active_user"

  private val fields = "day,channel,pv,uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?)"

  private val deleteSql = s" "

  def main(args: Array[String]) {

    ModuleClass.executor(PromotionActiveUser, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val loadUserPath = "/log/dbsnapshot/parquet/20161019/moretv_mtv_account"

        val loadPath = "/log/medusaAndMoretvMerger/2016{03*,04*,05*,06*,07*,08*,09*,10*}/*"

        val promotion = "'boshilian','dangbei','shafa','huanstore','xunma'"

        sqlContext.read.parquet(loadUserPath).filter("openTime > '2016-02-29'")
          .select("user_id")
          .repartition(200)
          .registerTempTable("log_user")

        sqlContext.read.parquet(loadPath)
          .filter(s"promotionChannel in ($promotion)")
          .filter("date between '2016-02-29' and '2016-10-18'")
          .select("date", "promotionChannel", "userId")
          .registerTempTable("log_data")


        val df = sqlContext.sql(
          "select data.date, data.promotionChannel , count(data.userId) as pv, count(distinct data.userId) as uv " +
            "from log_data as data inner join log_user as user on data.userId = user.user_id " +
            "group by data.date, data.promotionChannel ")

        df.show(40, false)

        val rdd = df.map(e => (e.getString(0), e.getString(1), e.getLong(2), e.getLong(3)))

        if (p.outputFile != "") {
          val conf = sc.hadoopConfiguration

          val fs = org.apache.hadoop.fs.FileSystem.get(conf)

          if (fs.exists(new org.apache.hadoop.fs.Path(p.outputFile))) {
            rdd.saveAsTextFile(p.outputFile)
          }

          else println("file already exists")

        }

        //   val rdd = df.map(e => (e.getString(0), e.getString(1), e.getLong(2), e.getLong(3)))
      }
      case None => {

      }
    }


  }

}
