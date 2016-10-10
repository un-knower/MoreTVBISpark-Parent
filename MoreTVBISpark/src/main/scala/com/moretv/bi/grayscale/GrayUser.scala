package com.moretv.bi.grayscale

import java.sql.DriverManager

import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

/**
  * Created by Will on 2016/5/12.
  */
object GrayUser extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(GrayUser,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val day = DateFormatUtils.toDateCN(p.startDate, -1)
        val util = new DBOperationUtils("tvservice")
        val ids = util.selectOne(s"SELECT MIN(id),MAX(id) FROM tvservice.mtv_account WHERE openTime <= '$day 23:59:59'")

        val sampleRdd = new JdbcRDD(sc, () => {
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
            "bi", "mlw321@moretv")
        },
          s"SELECT product_model,user_id FROM `mtv_account` WHERE ID >= ? AND ID <= ? " +
            s"and openTime between <= '$day 23:59:59' and right(mac,2) in ('26','BC')",
          ids(0).toString.toLong,
          ids(1).toString.toLong,
          10,
          r => (r.getString(1), r.getString(2))).filter(_ != null).distinct()
      }
      case None =>
        throw new RuntimeException("At least need param --startDate.")

    }
  }
}
