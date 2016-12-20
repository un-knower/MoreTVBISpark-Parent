package com.moretv.bi.temp.user

import java.util.Calendar
import java.lang.{Long => JLong}

import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.commons.httpclient.util.DateUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by witnes on 10/20/16.
  */
object PromotionActiveUser extends BaseClass {

  private val tableName = "promotion_active_user"

  private val fields = "day,channel,pv,uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = '?'"

  def main(args: Array[String])  {

    ModuleClass.executor(PromotionActiveUser, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val promotion = "'boshilian','dangbei','shafa','huanstore','xunma'"

        val loadUserPath = s"/log/dbsnapshot/parquet/${p.endDate}/moretv_mtv_account"

        val cal = Calendar.getInstance

        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        var loadDate = DateFormatUtils.readFormat.format(cal.getTime)

        cal.add(Calendar.DAY_OF_YEAR, -1)

        var sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)


        sqlContext.read.parquet(loadUserPath).filter(s"openTime > $sqlDate")
          .select("user_id")
          .repartition(200)
          .registerTempTable("log_user")

        var rdd: RDD[(String, String, Long, Long)] = sc.emptyRDD

        while (loadDate <= p.endDate) {

          val start = System.currentTimeMillis()

          val loadPath = s"/log/medusaAndMoretvMerger/$loadDate/*/*"
          sqlContext.read.parquet(loadPath)
            .filter(s"promotionChannel in ($promotion)")
            .select("date", "promotionChannel", "userId")
            .registerTempTable("log_data")

          val df = sqlContext.sql(
            s"""
               |select data.date, data.promotionChannel , count(data.userId) as pv, count(distinct data.userId) as uv
               | from log_data as data inner join log_user as user on data.userId = user.user_id
               | where data.date = '$sqlDate'
               | group by data.date, data.promotionChannel
            """.stripMargin)

          val tempRdd = df.map(e => ((e.getString(0), e.getString(1), e.getLong(2), e.getLong(3))))
          rdd = rdd.union(tempRdd)

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }
          df.collect.foreach(w => {
            util.insert(insertSql, w.getString(0), w.getString(1), new JLong(w.getLong(2)), new JLong(w.getLong(3)))
          })

          cal.add(Calendar.DAY_OF_YEAR, 1)
          sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_YEAR, 1)
          loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_YEAR, -1)
        }

        val rddStr = rdd.map(x => (x._1 + x._2 + x._3 + x._4 + ""))
        rdd.saveAsObjectFile(p.outputFile)

        if (p.outputFile != "") {
          val conf = sc.hadoopConfiguration

          val fs = org.apache.hadoop.fs.FileSystem.get(conf)

          if (fs.exists(new org.apache.hadoop.fs.Path(p.outputFile))) {
            println("file already exists")
          }

          else rddStr.saveAsObjectFile(p.outputFile)


        }


      }
      case None => {

      }
    }


  }

}
