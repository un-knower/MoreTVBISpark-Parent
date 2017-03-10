package com.moretv.bi.temp

import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
 * Created by Administrator on 2017/1/5.
 */
object LauncherClickByCityInfo extends BaseClass{

  private val regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r
  private val keyCity = Array("""'上海'""","""'北京'""","""'广州'""").mkString(",")
  private val importCityA = Array("""'深圳'""","""'沈阳'""","""'南京'""","""'成都'""").mkString(",")
  private val importCityB = Array("""'大连'""","""'长沙'""","""'武汉'""","""'青岛'""","""'杭州'""","""'天津'""","""'重庆'""",
                                  """'西安'""","""'苏州'""","""'宁波'""","""'合肥'""","""'济南'""","""'昆明'""").mkString(",")
  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) = {
    sqlContext.udf.register("getContentType", getContentType _)
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val areaDir = s"${LogTypes.USERAREAOUTDIR}20170309/userArea/"
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val insertSql = "insert into tmp_ams_launcher_click_by_city_info(day,access_area,access_location,city_type,city,num) values(?,?,?,?,?,?)"
        val insertSql1 = "insert into tmp_ams_subject_view_by_city_info(day,content_type,city_type,city,num) values(?,?,?,?,?)"
        val deleteSql = "delete from tmp_ams_launcher_click_by_city_info where day=?"
        val deleteSql1 = "delete from tmp_ams_subject_view_by_city_info where day=?"
        sqlContext.read.load(areaDir).registerTempTable("log_data")
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))
        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val insertDay = DateFormatUtils.toDateCN(date,-1)
          cal.add(Calendar.DAY_OF_YEAR,-1)
          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.HOMEACCESS,date).registerTempTable("log_data1")
          DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.SUBJECT,date).registerTempTable("log_data2")
          if(p.deleteOld){
            util.delete(deleteSql,insertDay)
            util.delete(deleteSql1,insertDay)
          }
          sqlContext.sql(
            s"""
              |select a.accessArea,a.locationIndex,case when b.city in (${keyCity}) then '核心城市'
              |when b.city in (${importCityA}) then '重点城市A' when b.city in (${importCityB}) then '重点城市B' else '其他' end as cityType,
              |b.city,count(a.userId)
              |from log_data1 as a
              |join log_data as b
              |on a.userId=b.user_id
              |where a.accessArea in ('classification','recommendation')
              |group by a.accessArea,a.locationIndex,case when b.city in (${keyCity}) then '核心城市'
              |when b.city in (${importCityA}) then '重点城市A' when b.city in (${importCityB}) then '重点城市B' else '其他' end,
              |b.city
            """.stripMargin).map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getLong(4))).collect().foreach(e=>{
            util.insert(insertSql,insertDay,e._1,e._2,e._3,e._4,e._5)
          })

          sqlContext.sql(
            s"""
              |select getContentType(a.subjectCode),case when b.city in (${keyCity}) then '核心城市'
              |when b.city in (${importCityA}) then '重点城市A' when b.city in (${importCityB}) then '重点城市B' else '其他' end as cityType,
              |b.city,count(a.userId)
              |from log_data2 as a
              |join log_data as b
              |on a.userId = b.user_id
              |where a.event = 'enter'
              |group by getContentType(a.subjectCode),case when b.city in (${keyCity}) then '核心城市'
              |when b.city in (${importCityA}) then '重点城市A' when b.city in (${importCityB}) then '重点城市B' else '其他' end,
              |b.city
            """.stripMargin).map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getLong(3))).collect().foreach(e=>{
            util.insert(insertSql1,insertDay,e._1,e._2,e._3,e._4)
          })
        })



      }
      case None => throw new RuntimeException("At least needs one param: startDate!")
    }
  }

  def getContentType(subjectCode:String) = {
    regex findFirstMatchIn subjectCode match {
      case Some(e) => fromEngToChinese(e.group(1))
      case _ => "未知"
    }
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
      case "mv"=>"音乐"
      case _ => "未知"
    }
  }

}
