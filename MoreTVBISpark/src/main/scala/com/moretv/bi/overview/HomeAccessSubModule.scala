package com.moretv.bi.overview

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object HomeAccessSubModule extends BaseClass with DateUtil{

  def main(args: Array[String]) {
    config.setAppName("HomeAccessSubModule")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val df = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.HOMEACCESS)
        val resultRDD = df.filter("event = 'enter'").select("date","accessArea","accessLocation","userId").map(e =>(e.getString(0),e.getString(1),e.getString(2),e.getString(3))).
                        map(e=>(getKeys(e._1,e._2,e._3),e._4)).filter(e=>e._1._5!=null).persist(StorageLevel.MEMORY_AND_DISK)
        val userNum = resultRDD.distinct().countByKey()
        val accessNum = resultRDD.countByKey()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from home_access_submodule where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO home_access_submodule(year,month,day,region_code,region_name,subModuleCode,user_num,access_num) VALUES(?,?,?,?,?,?,?,?)"
        userNum.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,x._1._5,x._1._6,new Integer(x._2.toInt),new Integer(accessNum(x._1).toInt))
        })
        resultRDD.unpersist()
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, accessArea:String, accessLocation:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    val names = Map(
      "0"->"首页总计",
      "1"->"热门推荐",
      "2"->"卫视直播",
      "3"->"大家在看",
      "4"->"观看历史",
      "5"->"分类入口",
      "6"->"节目类",
      "101"->"热门推荐第一屏",
      "102"->"热门推荐第二屏",
      "103"->"热门推荐第三屏"
    )
    val areaName = names.getOrElse(accessArea,null)

    (year,month,date,accessArea,areaName,"Location_"+accessLocation)
  }
}
