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
object UserByProductBrandDistribution extends BaseClass {
  private val tableName = "medusa_user_product_brand_distribution"
  private val insertSql = s"insert into ${tableName}(day,brand_name,new_user_num,total_user_num) values (?,?,?,?)"
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
          val inputDate = DateFormatUtils.enDateAdd(date,-1)
          val insertDate = DateFormatUtils.toDateCN(inputDate,0)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          DataIO.getDataFrameOps.getDimensionDF(sc, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PRODUCT_MODEL).
            registerTempTable(DimensionTypes.DIM_MEDUSA_PRODUCT_MODEL)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT,inputDate).
            registerTempTable(LogTypes.MORETV_MTV_ACCOUNT)


          /**统计新增用户的终端品牌分布*/
          val newUserRdd = sqlContext.sql(
            s"""
              |select case when c.brand_name is null then '未知' else c.brand_name end,count(distinct c.mac)
              |from
              |(
              |select a.mac,b.brand_name
              |from ${LogTypes.MORETV_MTV_ACCOUNT} as a
              |left join ${DimensionTypes.DIM_MEDUSA_PRODUCT_MODEL} as b
              |on a.product_model = b.product_model
              |where a.openTime between '$insertDate 00:00:00' and '$insertDate 23:59:59'
              |) as c
              |group by case when c.brand_name is null then '未知' else c.brand_name end
            """.stripMargin).map(e=>(e.getString(0),e.getLong(1)))


          /**统计累计用户的终端品牌分布*/
          val totalUserRdd = sqlContext.sql(
            s"""
               |select case when c.brand_name is null then '未知' else c.brand_name end,count(distinct c.mac)
               |from
               |(
               |select a.mac,b.brand_name
               |from ${LogTypes.MORETV_MTV_ACCOUNT} as a
               |left join ${DimensionTypes.DIM_MEDUSA_PRODUCT_MODEL} as b
               |on a.product_model = b.product_model
               |where a.openTime <= '$insertDate 23:59:59'
               |) as c
               |group by case when c.brand_name is null then '未知' else c.brand_name end
            """.stripMargin).map(e=>(e.getString(0),e.getLong(1)))

          if(p.deleteOld){
            util.delete(deleteSql,insertDate)
          }

          newUserRdd.join(totalUserRdd).map(e=>(e._1,e._2._1,e._2._2)).collect().foreach(i=>{
            util.insert(insertSql,insertDate,i._1,i._2,i._3)
          })

        })
      }

      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }

}
