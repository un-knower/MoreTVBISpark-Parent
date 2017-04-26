package com.moretv.bi.whiteMedusaVersionEstimate

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by yang.qizhen on 2017/4/20.
  * 用户升级情况的统计
  * 按日新版本升级人数
  * 按日新版本累计人数 & 累计人数占比
  */
object UserUpdateStatistic extends BaseClass {
  private val tableName = "whiteMedusa_user_update_statistic"
  private val insertSql = s"insert into ${tableName}(day,total_num,proportion) values (?,?,?)"
  private val deleteSql = s"delete from ${tableName} where day = ?"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    val tmpSqlContext = sqlContext
    import tmpSqlContext.implicits._
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        calendar.add(Calendar.DAY_OF_MONTH,-1)
        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, 0)
          calendar.add(Calendar.DAY_OF_MONTH, -1)

          DataIO.getDataFrameOps.getDF(sc, p.paramMap, DBSNAPSHOT, LogTypes.MORETV_MTV_ACCOUNT, date).
            registerTempTable(LogTypes.MORETV_MTV_ACCOUNT)


          sqlContext.sql(
            s"""
              |select current_version,openTime,mac
              |from ${LogTypes.MORETV_MTV_ACCOUNT}
              |where current_version is not null and openTime is not null
            """.stripMargin).map(e=>{
            var version:String = null
            if(e.getString(0).contains("_")){
              version = e.getString(0).substring(e.getString(0).lastIndexOf("_")+1)
            }
            (version,e.getString(1),e.getString(2))
          }).toDF("current_version","openTime","mac").registerTempTable("log")


          /** 用户升级情况的统计 **/
          val updateUserCnt = sqlContext.sql(
            s"""
               |select count(distinct case when current_version = '3.1.4'
               |       and openTime <= '$insertDate 23:59:59' then mac end) as total_num,
               |       count(distinct case when current_version = '3.1.4' then mac end)/count(distinct mac) as proportion
               |from log
            """.stripMargin).map(e=>(e.getLong(0),e.getDouble(1)))

          if(p.deleteOld) util.delete(deleteSql,insertDate)

          updateUserCnt.collect.foreach(e => {
            println(s"The result is ${e._1}, ${e._2}")
            util.insert(insertSql, insertDate,e._1, e._2)
          })
        })
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }
}