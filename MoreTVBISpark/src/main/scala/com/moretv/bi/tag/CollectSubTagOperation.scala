package com.moretv.bi.tag

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by laishun on 15/10/9.
 */
object CollectSubTagOperation extends BaseClass with DateUtil{
  def main(args: Array[String]) {
    config.setAppName("CollectSubTagOperation")
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {

    ParamsParseUtil.parse(args) match {
      case Some(p) =>{

        val df = DataIO.getDataFrameOps.getDF(sc,p.paramMap,MORETV,LogTypes.COLLECT,p.startDate)
        val resultRDD = df.filter("collectType='tag'").select("date","collectContent","event").map(e =>(e.getString(0),e.getString(1),e.getString(2))).
                           map(e=>(getKeys(e._1,e._2),e._3)).groupByKey().map(e =>(e._1,countOKAndCancle(e._2))).collect()

        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        //delete old data
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from collectSubTagOperation where day = '$date'"
          util.delete(oldSql)
        }
        //insert new data
        val sql = "INSERT INTO collectSubTagOperation(year,month,day,tagName,ok,cancel) VALUES(?,?,?,?,?,?)"
        resultRDD.foreach(x =>{
          util.insert(sql,new Integer(x._1._1),new Integer(x._1._2),x._1._3,x._1._4,new Integer(x._2._1),new Integer(x._2._2))
        })
      }
      case None =>{
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }

  def getKeys(date:String, event:String)={
    val year = date.substring(0,4)
    val month = date.substring(5,7).toInt

    val tagName = URLDecoder.decode(event,"UTF-8")

    (year,month,date,tagName)
  }

  def countOKAndCancle(events:Iterable[String])={
    var ok =0;
    var cancle = 0;
    events.foreach(x =>{
      if(x == "ok"){
        ok = ok + 1
      }else if(x == "cancel"){
        cancle = cancle + 1
      }
    })
    (ok, cancle)
  }
}
