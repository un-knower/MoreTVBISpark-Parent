package com.moretv.bi.report.medusa.entrance

import java.lang.{Double => JDouble, Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import cn.whaley.sdk.parse.ReadConfig
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.report.medusa.util.FilesInHDFS
import com.moretv.bi.report.medusa.util.udf.PathParser
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by baozhi.wang on 2017/3/20.
  * 脚本作用：统计不同入口播放统计 【频道及栏目编排-频道概况-「电影」-不同入口播放统计】
  * 基本逻辑：如果播放节目是属于subject，则按照专题code来归类，否则，按照contentType归类
  *
  * 使用的维度表：
  *           专题维度表           dim_medusa_subject
  *           节目维度表           dim_medusa_program
  *           固定入口维度表        dim_medusa_fix_entrance_info
  * 使用的分析字段（从事实表获得）:
  *             userId           度量值
  *             duration         度量值
  *             videoSid         用来关联dim_medusa_program获得contentType
  *             subjectCode      事实表pathSpecial(3.x)和path(2.x)解析出的字段,在事实表ETL过程中会给出
  *             path             解析出入口区域launcher_area,入口位置launcherAccessLocation
  *             pathMain         解析出入口区域launcher_area,入口位置launcherAccessLocation
  *
  */
object ChannelEntrancePlayStatETL extends BaseClass {
  private val tableName = "contenttype_play_src_stat"
  private val fields = "day,contentType,entrance,pv,uv,duration"
  private val sqlInsert = s"insert into $tableName($fields) values(?,?,?,?,?,?)"
  private val deleteSql = s"delete from $tableName where day = ? "
  private val playNumLimit = 5000
  private val spark_df_analyze_table = "analyze_table"
  private val isDebug = false

  def main(args: Array[String]) {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sqlContext = new SQLContext(sc)
        println("---------------------"+ReadConfig.getConfig)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        sqlContext.udf.register("getSubjectCode", PathParser.getSubjectCodeByPathETL _)
        sqlContext.udf.register("getSubjectName", PathParser.getSubjectNameByPathETL _)
        sqlContext.udf.register("getSubjectType", PathParser.getSubjectTypeByPathETL _)
        sqlContext.udf.register("getEntranceType", PathParser.getEntranceTypeByPathETL _)

        //引入维度表
        val dimension_subject_input_dir =DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT)
        val dimensionSubjectFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_subject_input_dir)
        val dimension_program_input_dir =DataIO.getDataFrameOps.getDimensionPath(MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PROGRAM)
        val dimensionProgramFlag = FilesInHDFS.IsInputGenerateSuccess(dimension_program_input_dir)
        println(s"--------------------dimensionSubjectFlag is ${dimensionSubjectFlag}")
        println(s"--------------------dimensionProgramFlag is ${dimensionProgramFlag}")
        if (dimensionSubjectFlag && dimensionProgramFlag) {
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_SUBJECT).registerTempTable(DimensionTypes.DIM_MEDUSA_SUBJECT)
          DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PROGRAM).registerTempTable(DimensionTypes.DIM_MEDUSA_PROGRAM)
        }else{
          throw new RuntimeException(s"--------------------dimension not exist")
        }

        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        var sqlStr = ""
        (0 until p.numOfDays).foreach(w => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH, -1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          /**最终此逻辑会合并进入事实表的ETL过程-start*/
          /**事实表数据处理步骤
            * 1.过滤单个用户播放单个视频量过大的情况
            * 2.为事实表生成完整的subject code
            */
          val medusa_input_dir = DataIO.getDataFrameOps.getPath(MEDUSA, LogTypes.PLAY, date)
          val moretv_input_dir = DataIO.getDataFrameOps.getPath(MORETV, LogTypes.PLAYVIEW, date)
          val medusaFlag = FilesInHDFS.IsInputGenerateSuccess(medusa_input_dir)
          val moretvFlag = FilesInHDFS.IsInputGenerateSuccess(moretv_input_dir)
          if (medusaFlag && moretvFlag) {
            //step1
            DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.PLAY, date).registerTempTable("medusa_table")
            DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MORETV, LogTypes.PLAYVIEW, date).registerTempTable("moretv_table")
            sqlStr = """
                       |select userId,
                       |       videoSid,
                       |       pathSpecial,
                       |       pathMain,
                       |       getEntranceType(pathMain,'medusa') as entryType,
                       |       duration,
                       |       event,
                       |       'medusa'   as flag
                       |from medusa_table
                     """.stripMargin
            println("--------------------"+sqlStr)
            val medusa_table_rdd=sqlContext.sql(sqlStr).toJSON


            sqlStr = """
                       |select userId,
                       |       videoSid,
                       |       path,
                       |       getEntranceType(path,'moretv') as entryType,
                       |       duration,
                       |       event,
                       |       'moretv'   as flag
                       |from moretv_table
                     """.stripMargin
            val moretv_table_rdd=sqlContext.sql(sqlStr).toJSON
            val mergerRDD = medusa_table_rdd.union(moretv_table_rdd)
            val step1_table_df = sqlContext.read.json(mergerRDD)
            step1_table_df.registerTempTable("step1_table")
            writeToHDFSForCheck(date,"step1_table_df",step1_table_df,p.deleteOld)

            //step2 filter
            //用于过滤单个用户播放当个视频量过大的情况
            sqlStr = s"""
                        |select concat(userId,videoSid) as filterColumn
                        |from step1_table
                        |group by concat(userId,videoSid)
                        |having count(1)>=$playNumLimit
                     """.stripMargin
            println("--------------------"+sqlStr)
            sqlContext.sql(sqlStr).registerTempTable("step2_table_filter")

            sqlStr = s"""
                        |select a.userId,
                        |       a.videoSid,
                        |       a.pathSpecial,
                        |       a.pathMain,
                        |       a.path,
                        |       a.entryType,
                        |       a.duration,
                        |       a.event,
                        |       a.flag
                        |from step1_table           a
                        |     left join
                        |     step2_table_filter    b
                        |     on concat(a.userId,a.videoSid)=b.filterColumn
                        |where b.filterColumn is null
                     """.stripMargin
            println("--------------------"+sqlStr)
            val step2_table_df=sqlContext.sql(sqlStr)
            step2_table_df.cache()
            step2_table_df.registerTempTable("step2_table")
            writeToHDFSForCheck(date,"step2_table_df",step2_table_df,p.deleteOld)

            //step3
            sqlStr = """
                       |select userId,
                       |       videoSid,
                       |       pathMain,
                       |       path,
                       |       entryType,
                       |       duration,
                       |       event,
                       |       getSubjectType(pathSpecial,'medusa')                                               as subjectType,
                       |       if(getSubjectType(pathSpecial,'medusa')='subject',getSubjectName(pathSpecial),'')  as subjectName,
                       |       if(getSubjectType(pathSpecial,'medusa')='subject',getSubjectCode(pathSpecial,'medusa'),'')  as subjectCode
                       |from step2_table
                       |where flag='medusa'
                     """.stripMargin
            println("--------------------"+sqlStr)
            val medusa_table_init_df=sqlContext.sql(sqlStr)
            medusa_table_init_df.registerTempTable("medusa_table_init_table")
            writeToHDFSForCheck(date,"medusa_table_init_df",medusa_table_init_df,p.deleteOld)


            sqlStr = s"""
                        |select a.userId,
                        |       a.videoSid,
                        |       a.pathMain,
                        |       a.path,
                        |       a.entryType,
                        |       a.duration,
                        |       a.event,
                        |       a.subjectType,
                        |       if((a.subjectCode is null or a.subjectCode=''),b.subject_code,a.subjectCode) as subjectCode
                        |from medusa_table_init_table a
                        |left join
                        |    (select subject_name,
                        |            first(subject_code) as subject_code
                        |     from
                        |     ${DimensionTypes.DIM_MEDUSA_SUBJECT} where dim_invalid_time is null
                        |     group by subject_name
                        |    ) b
                        |on trim(a.subjectName)=trim(b.subject_name)
                     """.stripMargin
            println("--------------------"+sqlStr)
            val medusa_log_df=sqlContext.sql(sqlStr)
            medusa_log_df.cache()
            writeToHDFSForCheck(date,"medusa_log_df",medusa_log_df,p.deleteOld)

            sqlStr = """
                       |select userId,
                       |       videoSid,
                       |       pathMain,
                       |       path,
                       |       entryType,
                       |       duration,
                       |       event,
                       |       getSubjectType(path,'moretv')  as subjectType,
                       |       getSubjectCode(path,'moretv')  as subjectCode
                       |from step2_table
                       |where flag='moretv'
                      """.stripMargin
            println("--------------------"+sqlStr)
            val moretv_log_df = sqlContext.sql(sqlStr)
            moretv_log_df.cache()
            writeToHDFSForCheck(date,"moretv_log_df",moretv_log_df,p.deleteOld)

            //moretv and medusa 获得subjectCode后，合并
            val moretv_medusa_log_df =medusa_log_df.unionAll(moretv_log_df)
            moretv_medusa_log_df.cache()
            moretv_medusa_log_df.registerTempTable("moretv_medusa_log_df_table")
            writeToHDFSForCheck(date,"moretv_medusa_log_df",moretv_medusa_log_df,p.deleteOld)

            //通过关联DIM_MEDUSA_PROGRAM维度表获得videoSid对应的contentType
            sqlStr = s"""
                        |select a.userId,
                        |       a.videoSid,
                        |       a.pathMain,
                        |       a.path,
                        |       a.entryType,
                        |       a.duration,
                        |       a.event,
                        |       a.subjectType,
                        |       a.subjectCode,
                        |       b.content_type_name,
                        |       c.subject_content_type_name
                        |from moretv_medusa_log_df_table a
                        |left join
                        |     (select * from ${DimensionTypes.DIM_MEDUSA_PROGRAM} where dim_invalid_time is null) b
                        |     on trim(a.videoSid)=trim(b.sid)
                        |left join
                        |     (select * from ${DimensionTypes.DIM_MEDUSA_SUBJECT} where dim_invalid_time is null) c
                        |     on trim(a.subjectCode)=trim(c.subject_code)
                        |where a.path is not null or a.pathMain is not null
                     """.stripMargin
            println("--------------------"+sqlStr)
            val step3_table_df = sqlContext.sql(sqlStr)
            step3_table_df.cache()
            writeToHDFSForCheck(date,"step3_table_df",step3_table_df,p.deleteOld)
            step3_table_df.registerTempTable(spark_df_analyze_table)
        }else {
            throw new RuntimeException("2.x and 3.x log data is not exist")
          }
          /*最终此逻辑会合并进入事实表的ETL过程-end*/

          /*进入分析代码*/
          sqlStr = s"""
                     |select if(subject_content_type_name is not null,subject_content_type_name,content_type_name) as contentType,
                     |        entryType,
                     |        count(userId)             as playNum,
                     |        count(distinct userId)    as playUser
                     |from $spark_df_analyze_table
                     |where event in ('$MEDUSA_EVENT_START_PLAY','$MORETV_EVENT_START_PLAY')
                     |group by if(subject_content_type_name is not null,subject_content_type_name,content_type_name),
                     |         entryType
                   """.stripMargin
          println("--------------------"+sqlStr)
          val channel_entry_playNum_playUser_df = sqlContext.sql(sqlStr)
          channel_entry_playNum_playUser_df.registerTempTable("channel_entry_playNum_playUser_df")
          writeToHDFSForCheck(date,"channel_entry_playNum_playUser_df",channel_entry_playNum_playUser_df,p.deleteOld)


          sqlStr = s"""
                      |select if(subject_content_type_name is not null,subject_content_type_name,content_type_name) as contentType,
                      |        entryType,
                      |        sum(duration)             as duration_sum
                      |from $spark_df_analyze_table
                      |where event not in ('$MEDUSA_EVENT_START_PLAY') and
                      |      duration between 1 and 10800
                      |group by if(subject_content_type_name is not null,subject_content_type_name,content_type_name),
                      |         entryType
                   """.stripMargin
          println("--------------------"+sqlStr)
          val channel_entry_duration_df = sqlContext.sql(sqlStr)
          channel_entry_duration_df.registerTempTable("channel_entry_duration_df")
          writeToHDFSForCheck(date,"channel_entry_duration_df",channel_entry_duration_df,p.deleteOld)

          sqlStr = s"""
                      |select  a.contentType,
                      |        a.entryType,
                      |        a.playNum,
                      |        a.playUser,
                      |        round(b.duration_sum/a.playUser) as avg_duration
                      |from channel_entry_playNum_playUser_df    a
                      |     join
                      |     channel_entry_duration_df            b
                      |     on a.contentType=b.contentType and
                      |        a.entryType=b.entryType
                   """.stripMargin
          println("--------------------"+sqlStr)
          val mysql_result_df = sqlContext.sql(sqlStr)
          writeToHDFSForCheck(date,"mysql_result_df",mysql_result_df,p.deleteOld)

          if (p.deleteOld) {
            util.delete(deleteSql, sqlDate)
          }
          //day,contentType,entrance,pv,uv,duration
          mysql_result_df.collect.foreach(row=>{
            var contentType=row.getString(0)
            if(null==contentType){
              contentType=row.getString(0)
            } else if(contentType.equalsIgnoreCase("电视剧")){
              contentType="电视"
            }else if(contentType.equalsIgnoreCase("记录片")){
              contentType="纪实"
            }
            util.insert(sqlInsert,sqlDate,contentType,row.getString(1),new JLong(row.getLong(2)),new JLong(row.getLong(3)),new JDouble(row.getDouble(4)))
          })
        })
      }
      case None => {
      }
    }
  }

  //用来写入HDFS，测试数据是否正确
  def writeToHDFSForCheck(date:String,logType:String,df:DataFrame,isDeleteOld:Boolean): Unit = {
    if (isDebug) {
      println(s"--------------------$logType is write done.")
      val outputPath = DataIO.getDataFrameOps.getPath(MERGER, logType, date)
      if (isDeleteOld) {
        HdfsUtil.deleteHDFSFile(outputPath)
      }
      df.write.parquet(outputPath)
    }
  }

  //dfUser: userId,pathMain,path,contentType,pathIdentificationFromPath,flag,cast(0 as Long)
  //dfDuration : userId,pathMain,path,contentType,pathIdentificationFromPath,flag,duration
  /**
    * 原有统计逻辑：如果播放节目是属于subject，则按照专题code所属的contentType来归类，否则，按照videoSid的contentType归类
    * pathIdentificationFromPath生成逻辑：
    * a.medusa日志格式[使用pathSpecial字段解析出来的pathIdentificationFromPath字段做判断，例如：]
    *                subject-新闻头条-hot11  -->  新闻头条-hot11
    *                subject-儿歌一周热播榜   -->  儿歌一周热播榜
    * b.helios日志格式[使用path字段解析出来的pathIdentificationFromPath字段做判断，例如：]
    *                home-history-subjectcollect-subject-hot260   -->     hot260
    *1.首先使用pathIdentificationFromPath字段与正则表达式match
    * if(pathIdentificationFromPath不为null){
    *    if(匹配){
    *      取出hot字段
    *    }else{
    *       //pathIdentificationFromPath字段可能是subjectName,例如：儿歌一周热播榜
    *       去mysql生成的subjectName->subjectCode的map获得subjectCode，与正则做匹配取出关键字，例如hot260 -> hot
    *       if(匹配){
    *           与正则做匹配取出关键字，例如hot260 -> hot
    *       }else{
    *           使用日志中的contentType【也就是sid对应的contentType字段】
    *       }
    *     }
    * }
    *
    * 生成的rdd格式为 channel【电影，电视剧等】,入口类型【分类入口，首页推荐等】,duration,userId
    * 过滤channel为非法类型的记录
    * 过滤入口类型为null的记录
    * */
  /*def contentFilter(df: DataFrame): RDD[(String, String, Long, String)] = {
    val rdd = df.map(e => {
      var channel = e.getString(3)
      if (e.getString(4) != null) {
        channel = regex findFirstMatchIn e.getString(4) match {
          case Some(p) => p.group(1)
          case None => {
            regex findFirstMatchIn codeMap.getOrElse(e.getString(4), e.getString(3)) match {
              case Some(pp) => pp.group(1)
              case None => e.getString(3)
            }
          }
        }
      }
      //                    pathMain        path             flag              0             userId
      (channel, splitSource(e.getString(1), e.getString(2), e.getString(5)), e.getLong(6), e.getString(0))

    })
      .filter(
        e => (e._1 == "movie" || e._1 == "kids" || e._1 == "tv" || e._1 == "sports" || e._1 == "kids"
          || e._1 == "reservation" || e._1 == "mv" || e._1 == "jilu" || e._1 == "comic" || e._1 == "zongyi"
          || e._1 == "hot" || e._1 == "xiqu"
          ))

      .filter(_._2 != null)
    rdd
  }*/


  /*def fromEngToChinese(str: String): String = {
    str match {
      case "movie" => "电影"
      case "tv" => "电视"
      case "hot" => "资讯短片"
      case "kids" => "少儿"
      case "zongyi" => "综艺"
      case "comic" => "动漫"
      case "jilu" => "纪实"
      case "sports" => "体育"
      case "xiqu" => "戏曲"
      case "mv" => "音乐"
      case _ => "未知"
    }
  }*/

  //pathMain        path             flag
  /*def splitSource(pathMain: String, path: String, flag: String): String = {
    val specialPattern = "home\\*my_tv\\*[a-zA-Z0-9&\\u4e00-\\u9fa5]{1,}".r
    flag match {
      case "medusa" => {
        sourceRe findFirstMatchIn pathMain match {
          case Some(p) => {
            p.group(1) match {
              case "home*classification" => "分类入口"
              case "home*my_tv*history" => "历史"
              case "home*my_tv*collect" => "收藏"
              case "home*recommendation" => "首页推荐"
              case "search" => "搜索"
              case _ => {
                if (specialPattern.pattern.matcher(p.group(1)).matches) {
                  "自定义入口"
                }
                else {
                  "其它3"
                }
              }
            }
          }
          case None => "其它3"
        }
      }
      case "moretv" => {
        sourceRe1 findFirstMatchIn path match {
          case Some(p) => {
            p.group(1) match {
              case "classification" => "分类入口"
              case "history" => "历史"
              case "hotrecommend" => "首页推荐"
              case "search" => "搜索"
              case _ => "其它2"
            }
          }
          case None => "其它2"
        }
      }
    }

  }*/



}