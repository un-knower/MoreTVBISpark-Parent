package com.moretv.bi.report.medusa.temporaryDemands.medusaGrayTestingDemands

import java.sql.DriverManager
import com.moretv.bi.util.{ParamsParseUtil, DBOperationUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/15.
 * 该对象用于统计每天升级apk的用户信息
 */
object updateUserIdDaily extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_TVSERVICE_MYSQL)
        val startDate=p.startDate
        val outputPath = s"/log/medusa/updateInfo/$startDate/userId"

        val minIdNum = util.selectOne("select min(id) from mtv_account")(0).toString.toLong
        val maxIdNum = util.selectOne("select max(id) from mtv_account")(0).toString.toLong
        val numOfPartition =20

        val todayUserRdd = new JdbcRDD(sc,
          ()=>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
          },
          "select distinct user_id from mtv_account where id >= ? and id <= ? and current_version='MoreTV_TVApp3" +
            ".0_Medusa_3.0.5' and product_model not in ('MagicBox_M13','M321','LetvNewC1S','we20s')",
          minIdNum,
          maxIdNum,
          numOfPartition,
          r=>(r.getString(1))
        )

        val mergeDate = todayUserRdd.map(e=>(startDate,e))

        val infoToDf = mergeDate.toDF("date","userId")
        infoToDf.write.parquet(outputPath)

      }
      case None=>{throw new RuntimeException("At least needs one param: StartDate!")}
    }
  }
}
