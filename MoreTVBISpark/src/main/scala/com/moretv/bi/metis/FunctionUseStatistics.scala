package com.moretv.bi.metis

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
 * Created by zhangyu on 2016/8/16.
 * 统计不同功能使用的人数/次数
 * 根据统计需求,需要读取多种日志格式,function已确定
 * table_name: function_use_statistics
 * (id,day,function,user_num,access_num)
 */
object FunctionUseStatistics extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(FunctionUseStatistics, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val util = DataIO.getMySqlOps("metis_bi_mysql")

        val calendar = Calendar.getInstance()
        val startDay = p.startDate
        calendar.setTime(DateFormatUtils.readFormat.parse(startDay))

        (0 until p.numOfDays).foreach(x => {

          val logDay = DateFormatUtils.readFormat.format(calendar.getTime)
          val sqlDay = DateFormatUtils.toDateCN(logDay, -1)

          if (p.deleteOld) {
            val deleteSql = "delete from function_use_statistics where day = ?"
            util.delete(deleteSql, sqlDay)
          }

          val insertSql = "insert into function_use_statistics(day,function,user_num,access_num) values(?,?,?,?)"

          //虚拟遥控器搜索
          val logPathVirtualController = s"/log/metis/parquet/$logDay/virtualcontroller"
          sqlContext.read.load(logPathVirtualController).select("userId").
          registerTempTable("log_data_virtualcontroller")

          val sqlRddVirtualController = sqlContext.sql("select count(distinct userId), " +
            "count(userId) from log_data_virtualcontroller").
          map(row=>{
            val user_num1 = row.getLong(0)
            val access_num1 = row.getLong(1)
            (user_num1,access_num1)
          })

          sqlRddVirtualController.collect().foreach(e=>{
            util.insert(insertSql,sqlDay,"虚拟遥控器搜索",e._1,e._2)
          })

          //离线缓存
          val logPathBuffer = s"/log/metis/parquet/$logDay/buffer"
          sqlContext.read.load(logPathBuffer).select("userId").
          registerTempTable("log_data_buffer")

          val sqlRddBuffer = sqlContext.sql("select count(distinct userId), " +
            "count(userId) from log_data_buffer").
            map(row=>{
              val user_num2 = row.getLong(0)
              val access_num2 = row.getLong(1)
              (user_num2,access_num2)
            })

          sqlRddBuffer.collect().foreach(e=>{
            util.insert(insertSql,sqlDay,"离线缓存",e._1,e._2)
          })

          //搜索
          val logPathSearch = s"/log/metis/parquet/$logDay/search"
          sqlContext.read.load(logPathSearch).select("userId").
            registerTempTable("log_data_search")

          val sqlRddSearch = sqlContext.sql("select count(distinct userId), " +
            "count(userId) from log_data_search").
            map(row=>{
              val user_num3 = row.getLong(0)
              val access_num3 = row.getLong(1)
              (user_num3,access_num3)
            })

          sqlRddSearch.collect().foreach(e=>{
            util.insert(insertSql,sqlDay,"搜索",e._1,e._2)
          })

          //收藏
          val logPathCollect = s"/log/metis/parquet/$logDay/collect"
          sqlContext.read.load(logPathCollect).select("userId","event").
            registerTempTable("log_data_collect")

          val sqlRddCollect = sqlContext.sql("select count(distinct userId), " +
            "count(userId) from log_data_collect " +
            "where event = 'ok'").
            map(row=>{
              val user_num4 = row.getLong(0)
              val access_num4 = row.getLong(1)
              (user_num4,access_num4)
            })

          sqlRddCollect.collect().foreach(e=>{
            util.insert(insertSql,sqlDay,"收藏",e._1,e._2)
          })

          //取消收藏
          val sqlRddCancelCollect = sqlContext.sql("select count(distinct userId), " +
            "count(userId) from log_data_collect " +
            "where event = 'cancel'").
            map(row=>{
              val user_num5 = row.getLong(0)
              val access_num5 = row.getLong(1)
              (user_num5,access_num5)
            })

          sqlRddCancelCollect.collect().foreach(e=>{
            util.insert(insertSql,sqlDay,"取消收藏",e._1,e._2)
          })

          //热词搜索
          val logPathSearchHotWord = s"/log/metis/parquet/$logDay/searchhotword"
          sqlContext.read.load(logPathSearchHotWord).select("userId").
            registerTempTable("log_data_searchhotword")

          val sqlRddSearchHotWord = sqlContext.sql("select count(distinct userId), " +
            "count(userId) from log_data_searchhotword").
            map(row=>{
              val user_num6 = row.getLong(0)
              val access_num6 = row.getLong(1)
              (user_num6,access_num6)
            })

          sqlRddSearchHotWord.collect().foreach(e=>{
            util.insert(insertSql,sqlDay,"热词搜索",e._1,e._2)
          })

          //播放
          val logPathPlay = s"/log/metis/parquet/$logDay/play"
          sqlContext.read.load(logPathPlay).select("userId","event").
            registerTempTable("log_data_play")

          val sqlRddPlay = sqlContext.sql("select count(distinct userId), " +
            "count(userId) from log_data_play " +
            "where event = 'startplay'").
            map(row=>{
              val user_num7 = row.getLong(0)
              val access_num7 = row.getLong(1)
              (user_num7,access_num7)
            })

          sqlRddPlay.collect().foreach(e=>{
            util.insert(insertSql,sqlDay,"播放",e._1,e._2)
          })

          //投屏
          val logPathScreenProject = s"/log/metis/parquet/$logDay/screenproject"
          sqlContext.read.load(logPathScreenProject).select("userId").
            registerTempTable("log_data_screenproject")

          val sqlRddScreenProject = sqlContext.sql("select count(distinct userId), " +
            "count(userId) from log_data_screenproject").
            map(row=>{
              val user_num8 = row.getLong(0)
              val access_num8 = row.getLong(1)
              (user_num8,access_num8)
            })

          sqlRddScreenProject.collect().foreach(e=>{
            util.insert(insertSql,sqlDay,"投屏",e._1,e._2)
          })
          
          //左右翻页,暂未定义


          calendar.add(Calendar.DAY_OF_MONTH, -1)
        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }
  }


}
