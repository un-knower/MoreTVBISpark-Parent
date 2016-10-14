package com.moretv.bi.user.location

import com.moretv.bi.util.IPLocationUtils.IPLocationDataUtil
import com.moretv.bi.util.{DBOperationUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

import java.lang.{Long => JLong}
/**
  * Created by witnes on 10/11/16.
  */

/**
  * 领域: 事件
  * 对象: 事件用户ip
  * 数据源: enter
  * 维度: 月, 地区
  * 特征提取: ip
  * 统计:
  */
object EventUserAreaStat extends BaseClass {

  private val dataSource = "enter,play"

  private val eventNames = "'enter','startplay'"

  private val tableName = "moretv_area_stat"

  private val periods = "07*,08*,09*,1001"

  private val periodsRegexp = "2016-0[789]-\\d+"

  private val fields = "event, month, province, city, pv, uv"

  private val insertSql = s"insert into $tableName($fields) values (?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where month regexp '$periodsRegexp' "

  def main(args: Array[String]) {

    ModuleClass.executor(EventUserAreaStat, args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {

        val util = new DBOperationUtils("medusa")

        sqlContext.udf.register("getProvinceCity", IPLocationDataUtil.getProvinceCity _)

        val loadPath = s"/log/medusa/parquet/2016{$periods}/{$dataSource}"

        sqlContext.read.parquet(loadPath)
          .select("date", "userId", "ip", "event")
          .filter(s"ip is not null and event in ($eventNames) and date regexp '$periodsRegexp'")
          .registerTempTable("log_data")

        val df = sqlContext.sql("select event, substring(date,7,1) as month, getProvinceCity(ip) as location, " +
          "count(userId) as pv , count(distinct userId) as uv from log_data " +
          "group by event, substring(date,7,1), getProvinceCity(ip)")

        if(p.deleteOld) {

          util.delete(deleteSql)
        }

        df.collect.foreach(w => {

          val event = w.getString(0)

          val month = w.getString(1)

          val province = w.getString(2).split("-")(0)

          val city = w.getString(2).split("-")(1)

          val pv = new JLong(w.getLong(3))

          val uv = new JLong(w.getLong(4))

          util.insert(insertSql, event, month, province, city, pv, uv)

        })

      }
      case None => {

      }

    }
  }
}
