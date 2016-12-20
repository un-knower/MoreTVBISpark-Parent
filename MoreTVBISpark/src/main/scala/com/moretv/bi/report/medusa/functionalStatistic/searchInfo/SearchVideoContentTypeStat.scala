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
  * 对象: 搜索结果视频 -搜索类型
  * 数据源: clickSearchResult
  * 维度: 天, 搜索视频, 搜索结果类型
  * 特征提取: resultSid, resultName, contentType
  * 过滤条件: apkVersion
  * 统计: 搜索视频,搜索结果类型, pv, uv
  * 输出: tbl:[ search_video_contenttype_stat ]
  *           (day, videoSid, videoName, contentType, pv, uv)
  */
object SearchVideoContentTypeStat extends  BaseClass{

  private val dataSource = "clickSearchResult"

  private val tableName = "search_video_contenttype_stat"

  private val fileds = "day,videoSid,videoName,contentType,pv,uv"

  private val insertSql = s"insert into $tableName ($fileds) values(?,?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ?"


  def main(args: Array[String]):Unit = {

    ModuleClass.executor(this,args)

  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {

      case Some(p) => {
        //init util
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(w => {

          //date
          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

         /* //path 
          val path = s"/log/medusa/parquet/$loadDate/$dataSource"
          println(path)


          //df 
          val df =sqlContext.read.parquet(path)
                    .select("resultSid", "resultName", "contentType", "userId", "apkVersion")
                      .filter("resultSid is not null")
                      .filter("resultName is not null")
                      .filter("contentType is not null")*/

          val df=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.CLICKSEARCHRESULT,loadDate).select("resultSid", "resultName", "contentType", "userId", "apkVersion")
            .filter("resultSid is not null")
            .filter("resultName is not null")
            .filter("contentType is not null")
          //rdd((resultSid,resultName,contentType),userId)
          val rdd = df.map(e=>((e.getString(0),e.getString(1),e.getString(2)),e.getString(3)))
                      .cache

          //aggregate
          val uvMap = rdd.distinct.countByKey
          val pvMap = rdd.countByKey

          //deal with table
          util.delete(deleteSql,sqlDate)
          uvMap.foreach(w=>{
            val key = w._1
            val uv = w._2
            val pv = pvMap.get(key) match {
              case Some(p) => p
              case _ => 0
            }
            util.insert(insertSql, sqlDate, w._1._1,w._1._2,w._1._3,new JLong(pv),new JLong(uv))
          })

        })

      }
      case None => {
        throw new Exception("at least one param for SearchVideoContentTypeStat")
      }
    }

  }
}
