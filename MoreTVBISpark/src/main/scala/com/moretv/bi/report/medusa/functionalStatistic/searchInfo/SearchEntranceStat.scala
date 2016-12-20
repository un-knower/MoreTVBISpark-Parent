package com.moretv.bi.report.medusa.functionalStatistic.searchInfo

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/18/16.
  */

/**
  * 领域: 搜索
  * 对象: 搜索入口
  * 数据源: searchEntrance
  * 维度: 天, 入口
  * 特征提取: entrance, userId
  * 过滤条件:
  * 统计: 入口 , pv, uv
  * 输出: tbl[ search_entrance_stat ]
  *         (day, entrance, pv, uv)
  */
object SearchEntranceStat extends BaseClass{

  private val dataSource = "searchEntrance"

  private val tableName = "search_entrance_stat"

  private val fields = "day, entrance, pv, uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]) {

    ModuleClass.executor(SearchEntranceStat,args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //init & util
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        //date
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w=>{
          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          val loadPath = s"/log/medusa/parquet/$loadDate/$dataSource"
          println(loadPath)

          //df
          val df = sqlContext.read.parquet(loadPath)
                      .select("userId","entrance")
                      .filter("entrance is not null")
          //rdd
          val rdd = df.map(e=>(e.getString(1),e.getString(0)))
                      .cache

          //aggregate
          val pvMap = rdd.countByKey
          val uvMap = rdd.distinct.countByKey

          //deal with table
          util.delete(deleteSql, sqlDate)

          uvMap.foreach(w=>{

            val key = w._1
            val uv = w._2
            val pv = pvMap.get(key) match {
              case Some(p) => p
              case _ => 0
            }
            util.insert(insertSql, sqlDate, key, new JLong(pv), new JLong(uv))

          })

        })
      }
      case None => {
        throw new Exception("SearchEntranceStat fails")
      }

    }
  }
}
