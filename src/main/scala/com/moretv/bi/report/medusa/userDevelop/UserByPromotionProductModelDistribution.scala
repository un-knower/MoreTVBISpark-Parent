package com.moretv.bi.report.medusa.userDevelop

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
 * Created by Chubby on 2016/9/7.
 */
object UserByPromotionProductModelDistribution extends BaseClass {
  private val tableName = "medusa_user_promotion_product_model_distribution"
  private val insertSql = s"insert into ${tableName}(day,promotion,product_model,new_user_num,total_user_num,dau) values (?,?,?,?,?,?)"
  private val deleteSql = s"delete from ${tableName} where day = ?"

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val loadDate = DateFormatUtils.enDateAdd(date,0)
          val inputDate = DateFormatUtils.enDateAdd(date,-1)
          val insertDate = DateFormatUtils.toDateCN(inputDate,0)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PRODUCT_MODEL).
            registerTempTable(DimensionTypes.DIM_MEDUSA_PRODUCT_MODEL)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,inputDate).
            registerTempTable(LogTypes.MORETV_MTV_ACCOUNT)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, MORETVLOGINLOG, LogTypes.LOGINLOG,loadDate).
            registerTempTable(LogTypes.LOGINLOG)


          /**统计新增用户的终端品牌分布*/

          sqlContext.sql(
            s"""
              |select a.promotion,a.product_model,count(distinct a.mac)
              |from ${LogTypes.MORETV_MTV_ACCOUNT} as a
              |where a.openTime between '$insertDate 00:00:00' and '$insertDate 23:59:59'
              |group by a.promotion,a.product_model
            """.stripMargin).toDF("promotion","product_model","new_user").registerTempTable("new_user_log")


          /**统计累计用户的终端品牌分布*/
          sqlContext.sql(
            s"""
               |select a.promotion,a.product_model,count(distinct a.mac)
               |from ${LogTypes.MORETV_MTV_ACCOUNT} as a
               |where a.openTime <= '$insertDate 23:59:59'
               |group by a.promotion,a.product_model
            """.stripMargin).toDF("promotion","product_model","total_user").registerTempTable("total_user_log")

          /**统计活跃用户分布*/
          sqlContext.sql(
            s"""
              |
              |select b.promotion,b.product_model,count(distinct a.mac)
              |from ${LogTypes.LOGINLOG} as a
              |join ${LogTypes.MORETV_MTV_ACCOUNT} as b
              |on a.mac = b.mac
              |where b.openTime <= '$insertDate 00:00:00'
            """.stripMargin).toDF("promotion","product_model","dau").registerTempTable("dau_log")

          /**计算总的promotion and product_model*/
          sqlContext.sql(
            """
              |select promotion,product_model
              |from new_user_log
              |union
              |select promotion,product_model
              |from total_user_log
              |union
              |select promotion,product_model
              |from dau_log
            """.stripMargin).distinct().registerTempTable("promotion_product_log")


          if(p.deleteOld){
            util.delete(deleteSql,insertDate)
          }

          sqlContext.sql(
            """
              |select a.promotion,a.product_model,b.new_user,c.total_user,d.dau
              |from promotion_product_log as a
              |left join new_user_log as b
              |on a.promotion = b.promotion and a.product_model = b.product_model
              |left join total_user_log as c
              |on a.promotion = c.promotion and a.product_model = c.product_model
              |left join dau_log as d
              |on a.promotion = d.promotion and a.product_model = d.product_model
            """.stripMargin).map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3),e.getLong(4))).collect().
            foreach(i=>{
            util.insert(insertSql,insertDate,i._1,i._2,i._3,i._4,i._5)
          })

        })
      }

      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

}
