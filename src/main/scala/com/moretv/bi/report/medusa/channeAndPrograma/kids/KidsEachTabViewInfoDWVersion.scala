package com.moretv.bi.report.medusa.channeAndPrograma.kids

/**
  * Created by witnes on 1/11/17.
  */

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, DimensionTypes, LogTypes}
import com.moretv.bi.logETL.KidsPathParser
import com.moretv.bi.report.medusa.channeAndPrograma.kids.KidsEachTabPlayInfoDWVersion.{MEDUSA, MEDUSA_DIMENSION, sqlContext}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}


/**
  * Created by witnes on 8/22/16.
  * 统计少儿频道各列表页(三级目录)的播放量
  */
object KidsEachTabViewInfoDWVersion extends BaseClass{

  private val tableName = "medusa_channel_eachtabwithgroup_view_kids_info"

  private val regex_collect = ("(kids_collect)*(观看历史|收藏追看|专题收藏)").r
  private val regex_kandonghua = ("(kandonghua|kids_anim)*(动画明星|热播推荐|最新出炉|动画专题|欧美精选|国产精选|0-3岁|" +
    "4-6岁|7-10岁|英文动画|少儿电影|儿童综艺|亲子交流|益智启蒙|童话故事|教育课堂|搞笑|机战|亲子|探险|最佳短片|猫猫优选|爱看动漫)").r
  private val regex_tingerge = ("(tingerge|kids_rhymes)*(随便听听|儿歌明星|儿歌热播|儿歌专辑|英文儿歌|舞蹈律动)").r
  private val regex_xuezhishi = ("(xuezhishi)*(4-7岁幼教|汉语学堂|英语花园|数学王国|安全走廊|百科世界|艺术宫殿|学习天地|趣味玩具|绘本故事)").r


  /*删除指定一天的所有记录*/
  private val deleteSql = s"delete from $tableName where day = ?"

  private val insertSql = s"insert into $tableName(day, channelname, groupname,tabname,view_num,view_user)" +
    "values (?,?,?,?,?,?)"

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this, args)
  }

  override def execute(args: Array[String]): Unit = {
    sqlContext.udf.register("getListCategoryMedusa", KidsPathParser.pathMainParse _)
    val sqlContextTemp = sqlContext
    import sqlContextTemp.implicits._
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap,MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE).
          registerTempTable(DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE)
        DataIO.getDataFrameOps.getDimensionDF(sqlContext, p.paramMap, MEDUSA_DIMENSION, DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP).
          registerTempTable(DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))
        var sqlStr = ""
        (0 until p.numOfDays).foreach(i=>{

          /*确定指定日期的数据源*/
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)

          DataIO.getDataFrameOps.getDF(sqlContext, p.paramMap, MEDUSA, LogTypes.TABVIEW, date).registerTempTable("medusa_table")

          sqlStr =
            """
              |select userId,
              |       pathMain,
              |       stationcode
              |from medusa_table
            """.stripMargin
          sqlContext.sql(sqlStr).map(e=>(e.getString(0),s"${e.getString(1)}*${e.getString(2)}")).toDF("userId","pathMain").registerTempTable("log")

          sqlStr =
            s"""
              |select a.userId,
              |       getListCategoryMedusa(a.pathMain,1) as main_category,
              |       getListCategoryMedusa(a.pathMain,2) as sub_category,
              |       getListCategoryMedusa(a.pathMain,3) as third_category,
              |       b.program_code
              |from log as a
              |join ${DimensionTypes.DIM_MEDUSA_PATH_PROGRAM_SITE_CODE_MAP} as b
              |on getListCategoryMedusa(a.pathMain,2) = b.path_code
              |where getListCategoryMedusa(a.pathMain,1) = 'kids'
            """.stripMargin

          sqlContext.sql(sqlStr).registerTempTable("step1_table_tmp")

          sqlStr =
            s"""
               |select a.userId,
               |       a.main_category,
               |       a.program_code,
               |       a.third_category,
               |       b.area_name
               |from step1_table_tmp as a
               |join ${DimensionTypes.DIM_MEDUSA_PAGE_ENTRANCE} as b
               |on a.program_code = b.area_code
              """.stripMargin
          val step1_table_df = sqlContext.sql(sqlStr).cache()
          step1_table_df.registerTempTable("step1_table")

          /** 最终此逻辑会合并进入事实表的ETL过程-end */
          sqlStr =
            s"""
               |select area_name,third_category,count(distinct userId) as platUser,count(userId) as playNum
               |from step1_table as a
               |group by area_name,third_category
            """.stripMargin
          val mysql_result_rdd = sqlContext.sql(sqlStr).map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3)))

          /*将统计结果集插入Mysql */
          if(p.deleteOld){
            util.delete(deleteSql,insertDate)
          }
          mysql_result_rdd.collect().foreach(e=>{
            try {
              util.insert(insertSql,insertDate,"少儿",e._1,e._2,
                new JLong(e._4), new JLong(e._3))
            } catch {
              case e: Exception => println(e)
            }
          })

        })
      }
      case None => {
        throw new RuntimeException("at least needs one param: startDate")
      }

    }
  }




}