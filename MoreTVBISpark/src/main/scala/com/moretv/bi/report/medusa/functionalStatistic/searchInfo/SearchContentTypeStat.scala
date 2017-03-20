package com.moretv.bi.report.medusa.functionalStatistic.searchInfo

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/14/16.
  */

/**
  * 领域: 搜索
  * 对象: 搜索结果类型
  * 数据源: clickSearchResult
  * 维度: 天, 搜索结果类型
  * 特征提取: contentType, userId
  * 过滤条件:
  * 统计: 搜索结果类型, pv, uv
  * 输出: tbl:[search_contenttype_stat]
  *           (day, contentType, pv, uv)
  */
object SearchContentTypeStat extends BaseClass{

  private val dataSource = "clickSearchResult"

  private val tableName = "search_contenttype_stat"

  private val fields = "day, contentType, pv, uv"

  private val insertSql = s"insert into $tableName($fields) values(?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ? "


  def main(args: Array[String]): Unit ={

      ModuleClass.executor(this,args)

  }
  override def execute(args: Array[String]): Unit = {

      ParamsParseUtil.parse(args) match {

        case Some(p) => {

          //util init
          val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          val startDate = p.startDate
          val cal = Calendar.getInstance
          cal.setTime(DateFormatUtils.readFormat.parse(startDate))

          (0 until p.numOfDays).foreach(w =>{

            //date
            val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
            cal.add(Calendar.DAY_OF_MONTH,-1)
            val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

            val df=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.SEARCH_CLICKRESULT,loadDate)
              .select("userId","contentType")
              .filter("contentType is not null")
            val rdd = df.map(e=>(e.getString(1),e.getString(0)))
                        .cache

            //aggregate
            val pvMap = rdd.countByKey
            val uvMap = rdd.distinct.countByKey

            if(p.deleteOld){
              util.delete(deleteSql, sqlDate)
            }

            //deal with table
            uvMap.foreach(w=>{
              val key = w._1
              val uv = w._2
              val pv = pvMap.get(key) match {
                case Some(p) => p
                case _ => 0
              }

              util.insert(insertSql, sqlDate,w._1, new JLong(pv), new JLong(uv))
            })
          })
        }
        case None => {
          throw new Exception("search_entrance_freq fails")
        }
      }

  }
}
