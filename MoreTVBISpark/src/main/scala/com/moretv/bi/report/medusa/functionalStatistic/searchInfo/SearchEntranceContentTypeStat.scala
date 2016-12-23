package com.moretv.bi.report.medusa.functionalStatistic.searchInfo

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/19/16.
  */

/**
  * 领域: 搜索
  * 对象: 搜索结果类型 - 各入口
  * 数据源: clickSearchResult
  * 维度: 天, 搜索入口, 搜索结果类型
  * 特征提取: entrance, contentType, userId
  * 统计: pv, uv
  * 输出: tbl[ search_entrance_contenttype_stat ]
  *         (day ,entrance, contenttype, pv, uv)
  */
object SearchEntranceContentTypeStat extends BaseClass{

  private val dataSource = "clickSearchResult"

  private val tableName =  "search_entrance_contenttype_stat"

  private val fields = "day, entrance, contenttype, pv, uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"

  def main(args: Array[String]) {

    ModuleClass.executor(this,args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        //init & util
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(w=>{
          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          //path
          /*val loadPath = s"/log/medusa/parquet/$loadDate/$dataSource"

          //df
          val df = sqlContext.read.parquet(loadPath)
                              .select("contentType", "userId", "entrance")
                              .filter("contentType is not null")
                              .filter("entrance is not null")*/
          val df=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.CLICKSEARCHRESULT,loadDate)
            .select("contentType", "userId", "entrance")
            .filter("contentType is not null")
            .filter("entrance is not null")

          //rdd ((entrance, contentType), userId)
          val rdd = df.map(e=>((e.getString(2),e.getString(0)),e.getString(1)))
                        .cache

          //aggregate
          val pvMap = rdd.countByKey
          val uvMap = rdd.distinct.countByKey

          //deal with table
          if(p.deleteOld){
            util.delete(deleteSql, sqlDate)
          }

          pvMap.foreach(w=>{
            val key = w._1
            val pv = w._2
            val uv = uvMap.get(key) match {
              case Some(p) => p
              case _ => 0
            }
            util.insert(insertSql,
              sqlDate,w._1._1,w._1._2,new JLong(pv), new JLong(uv))
          })

        })

      }
      case None => {
        throw  new Exception("SearchEntranceContentTypeStat fails")
      }
    }
  }
}
