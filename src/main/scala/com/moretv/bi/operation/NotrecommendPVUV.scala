package com.moretv.bi.operation

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import com.moretv.bi.util.SparkSetting
import org.apache.spark.sql.SQLContext
import java.lang.{Long => JLong}
import com.moretv.bi.util._


object NotrecommendPVUV extends BaseClass {
  def main(args: Array[String]) {
    config.setAppName("NotrecommendPVUV")
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //implicit val sQLContext = SQLContext.getOrCreate(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val date = DateFormatUtils.toDateCN(p.startDate, -1)
        val sql = "select date, userId from log_data where event = 'notrecommend'"

        val df_init = DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETV, LogTypes.OPERATION_ACW)
        df_init.registerTempTable("log_data")
        val df = sqlContext.sql(sql)
        val pv = df.count()
        val uv = df.distinct.count()

        if (p.deleteOld) {
          val sqlDelete = "DELETE FROM notrecommendPVUV WHERE day = ?"
          util.delete(sqlDelete, date)
        }

        val sqlInsert = "INSERT INTO notrecommendPVUV (day, user_num, user_access) VALUES(?,?,?)"
        util.insert(sqlInsert, date, new JLong(uv), new JLong(pv))
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
}
