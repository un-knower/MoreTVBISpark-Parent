package com.moretv.bi.dbmigration

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by 连凯 on 2017/7/22.
  * 初始化mtv_account_migration表，将2017-07-10之前的数据重新初始化一遍
  *
  */
object PromotionChannelDetailOpenInit extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {


        val sqlMinMaxId = "select min(id),max(id) from promotion_detail where day <= '2017-07-10'"
        val sqlData = "select id,year,month,day, promotion_channel,new_num from promotion_detail" +
          " where id >= ? and id <= ? and day <= '2017-07-10'"
        MySqlOps.getJdbcRDD(sc,DataBases.MORETV_EAGLETV_MYSQL,sqlMinMaxId,sqlData,600,rs => {
          s"(${rs.getLong(1)},${rs.getInt(2)},${rs.getInt(3)},'${rs.getString(4)}','${rs.getString(5)}',${rs.getLong(6)})"
        }).repartition(100).foreachPartition(par => {
          val db = DataIO.getMySqlOps(DataBases.MORETV_EAGLETV_MYSQL)
          val values = par.mkString(",")
          val insertSql = "insert into promotion_detail_open(id,year,month,day, promotion_channel,new_num) " +
            s"values $values"
          db.insert(insertSql)
          db.destory()
        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }
}
