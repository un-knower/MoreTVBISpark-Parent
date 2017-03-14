package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by baozhi.wang on 2017/3/13.
  * 统计不同频道的专题播放量，用于展示在各个频道的专题播放趋势图以及内容评估的专题趋势图
  *
  * 修改原有逻辑方式：首先从日志里直接获取subject code和subject name，然后load专题维度表dim_medusa_subject，
  * 通过sql的方式关联subject_code or subject_name获得subject_content_type_name
  *
  * 在play merge日志里面，有专门的subject_code和subject_name字段，但是没有用于分析脚本！？
  *
  *
  * CREATE EXTERNAL TABLE dim_medusa_subject(
  * subject_sk                     BIGINT,
  * subject_code                   STRING,
  * subject_name                   STRING,
  * subject_title                  STRING,
  * subject_content_type           STRING,
  * subject_content_type_name      STRING,
  * dim_valid_time                 TIMESTAMP,
  * dim_invalid_time               TIMESTAMP
  * )
  * STORED AS PARQUET
  * LOCATION '/data_warehouse/dw_dimensions/dim_medusa_subject';
  *
  * 数仓一期获得专题号的方式：
  * 在medusa日志格式里面
  * getSubjectCode(pathSpecial,'medusa')  作为 special_id
  * 调用MedusaSubjectNameCodeUtil.getSubjectCode(pathSpecial)，通过正则表达式regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r
  * 来解析出专题号【例如，subject-90后超模新势力-hot77，解析出hot77；subject-儿歌一周热播榜 解析出""】
  * 在moretv日志格式里面
  * getSubjectCode(path,'moretv') 作为 special_id
  * 调用那个非常复杂的正则表达式，获得专题号

  * 查看dim分支com.moretv.bi.report.medusa.channeAndPrograma.movie.EachChannelSubjectPlayInfo，看看分析代码是如何得到subject code的。
  * 首先使用merge后的日志，根据flag为medusa，pathPropertyFromPath为subjct的记录做操作，【pathIdentificationFromPath为，新闻头条-hot11】

  * 电视猫merge后，具体各个字段含义可参考
  * http://172.16.17.100:8090/pages/viewpage.action?pageId=4424612

  */
object EachChannelSubjectPlayInfoDim extends BaseClass{
  private val regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu)([0-9]+)""".r
  private val tableName="medusa_channel_subject_play_info_test"
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        sqlContext.udf.register("getSubjectCodeOrSubjectNameForMedusa",EachChannelSubjectPlayInfoDim.getSubjectCodeOrSubjectNameForMedusa _)
        sqlContext.udf.register("getSubjectCodeForMoretv",EachChannelSubjectPlayInfoDim.getSubjectCodeForMoretv _)

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          var sqlStr=""
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          //以2.x和3.x合并后的日志作为数据源
          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MERGER,LogTypes.PLAYVIEW,date).select("userId","launcherAreaFromPath","launcherAccessLocationFromPath",
            "pageDetailInfoFromPath","pathIdentificationFromPath","path","pathPropertyFromPath","flag","event")
            .registerTempTable("log_data")

          //--new added1
          //medusa日志
          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MERGER,LogTypes.PLAYVIEW,date)
            .select("userId","pathIdentificationFromPath","pathPropertyFromPath","flag","event")
            .filter("pathPropertyFromPath='subject' and flag='medusa' and event='startplay' ")
            .registerTempTable("medusa_subject_table")
          //get subject code and subject name use UDF function
          sqlStr=s"""
                    |select userId,
                    |       getSubjectCodeOrSubjectNameForMedusa(pathIdentificationFromPath) as subject_code,
                    |       getSubjectCodeOrSubjectNameForMedusa(pathIdentificationFromPath) as subject_name
                    |from medusa_subject_table
           """.stripMargin
          println(sqlStr)
          val medusa_subject_table_df=sqlContext.sql(sqlStr)
          //.registerTempTable("medusa_subject_table_v2")

          //moretv日志
          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MERGER,LogTypes.PLAYVIEW,date)
            .select("userId","path","event")
            .filter(" flag='moretv' and event='playview' ")
            .registerTempTable("helios_subject_table")
          //get subject code and subject name use UDF function
          sqlStr=s"""
                    |select userId,
                    |       getSubjectCodeForMoretv(path) as subject_code,
                    |       null as subject_name
                    |from helios_subject_table
           """.stripMargin
          println(sqlStr)
          val helios_subject_table_df=sqlContext.sql(sqlStr)
          //.registerTempTable("helios_subject_table_v2")

          medusa_subject_table_df.unionAll(helios_subject_table_df).registerTempTable("log_data")

          //dim_medusa_subject
          sqlStr=s"""
                    |select b.subject_content_type,
                    |       count(userId) as play_num,
                    |       count(distinct userId) as play_user,
                    |from log_data                 as a
                    |     join dim_medusa_subject  as b on concat(a.subject_code,a.subject_name)=concat(b.subject_code,b.subject_name)
                    |group by b.subject_content_type
           """.stripMargin
          println(sqlStr)

          if(p.deleteOld){
            val deleteSql=s"delete from $tableName where day=?"
            util.delete(deleteSql,insertDate)
          }
          val sqlInsert = s"insert into $tableName(day,channel_name,play_num,play_user) values (?,?,?,?)"
          sqlContext.sql(sqlStr).foreachPartition(partition => {
            partition.foreach(rdd => {
              //util.insert(sqlInsert,insertDate,e._1,new JLong(e._2),new JLong(e._3))
            })
          })

          //--new added end1

        })

      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }


  // 获取 专题code
  def getSubjectCodeOrSubjectNameForMedusa(subject:String) = {
    val regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r
    regex findFirstMatchIn subject match {
      // 如果匹配成功，说明subject包含了专题code，直接返回专题code
      case Some(m) => {
        m.group(1)+m.group(2)
      }
      // 没有匹配成功，说明subject为专题名称，不包含专题code，因此直接返回专题名称
      case None => subject
    }
  }

  def getSubjectCodeForMoretv(path:String):String = {
    var result:String=null
    if (path != null) {
      val info = SubjectUtils.getSubjectCodeAndPath(path)
      if (!info.isEmpty) {
        val subjectCode = info(0)
        result = subjectCode._1
      }
    }
    result
  }


  //TODO 解析出专题号后通过关联专题维度表获得专题类型
  def getSubjectTypeFromSubjectCode(subjectInfo:String,flag:String):String={
    if(subjectInfo!=null){
      if(flag=="moretv"){
        regex findFirstMatchIn subjectInfo match {
          case Some(m) => fromEngToChinese(m.group(1))
          case None => null
        }
      }else if(flag=="medusa"){
        val subjectCode=CodeToNameUtils.getSubjectCodeByName(subjectInfo)
        regex findFirstMatchIn subjectCode match {
          case Some(m) => fromEngToChinese(m.group(1))
          case None => null
        }
      }else null
    }else null

  }

  def fromEngToChinese(str:String):String={
    str match {
      case "movie"=>"电影"
      case "tv"=>"电视"
      case "hot"=>"资讯短片"
      case "kids"=>"少儿"
      case "zongyi"=>"综艺"
      case "comic"=>"动漫"
      case "jilu"=>"纪实"
      case "sports"=>"体育"
      case "xiqu"=>"戏曲"
    }
  }
}