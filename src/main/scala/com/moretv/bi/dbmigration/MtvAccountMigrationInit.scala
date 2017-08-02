package com.moretv.bi.dbmigration

import java.sql.DriverManager

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by 连凯 on 2017/7/22.
  * 初始化mtv_account_migration表，将2017-07-10之前的数据重新初始化一遍
  *
  */
object MtvAccountMigrationInit extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val tableName = p.paramMap.getOrElse("tableName","mtv_account_migration")

        DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,"20170721")
          .filter("openTime <= '2017-07-10 23:59:59'")
          .select("id","user_id","mac","wifi_mac","openTime","promotion_channel").map(row => {
          val id = row.getInt(0)
          val user_id = s"'${row.getString(1)}'"
          val mac = row.getString(2) match {
            case null => null
            case x => s"'${x.replace("\\","\\\\").replace("'","\\'")}'"
          }
          val wifi_mac = row.getString(3) match {
            case null => null
            case x => s"'${x.replace("\\","\\\\").replace("'","\\'")}'"
          }
          val openTime = s"'${row.getString(4)}'"
          val promotion_channel = row.getString(5) match {
            case null => null
            case x => s"'${x.replace("\\","\\\\").replace("'","\\'")}'"
          }
          val promotion_channel_origin = promotion_channel
          s"($id,$user_id,$mac,$wifi_mac,$openTime,$promotion_channel,$promotion_channel_origin)"
        }).repartition(20000).foreachPartition(par => {
          val db = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          Class.forName(db.driver)
          val values = par.mkString(",")
          val insertSql = s"insert into $tableName values " + values
          val conn = DriverManager.getConnection(db.url, db.user, db.password)
          val stmt = conn.createStatement()
          stmt.execute(insertSql)
          stmt.close()
          conn.close()
        })

        }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }
}
