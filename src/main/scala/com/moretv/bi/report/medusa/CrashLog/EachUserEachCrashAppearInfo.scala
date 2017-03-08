package com.moretv.bi.report.medusa.CrashLog

/**
 * Created by xiajun on 2016/9/21.
 * 该对象用于获取每天的crash的原始数据
 */
import java.lang.{Long => JLong}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ParamsParseUtil, _}
import org.apache.commons.codec.digest.DigestUtils


object EachUserEachCrashAppearInfo extends BaseClass{


  def main(args: Array[String]) {
    ModuleClass.executor(EachUserEachCrashAppearInfo,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate,-1)
        val insertSql = "insert into tmp_crash_info(day,mac,stackTraceMD5,productCode,dateCode,num) values(?,?,?,?,?,?)"
        // TODO 是否需要写到固定的常量类或者SDK读取
        if(p.deleteOld){
          val deleteSql = "delete from tmp_crash_info where day = ?"
          util.delete(deleteSql,day)
        }
//        DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.CRASH_LOG,inputDate).select(
//          "MAC","APP_VERSION_NAME","APP_VERSION_NAME","ANDROID_VERSION","STACK_TRACE","DATE_CODE","PRODUCT_CODE","CUSTOM_JSON_DATA"
//        ).map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),e.getString(5),e.getString(6),e.getString(7))).
//          map(log => (log._1.replace(":",""),log._2,log._3,log._4,log._5,log._6,log._7,log._8))
//          .filter(data => !DevMacUtils.macFilter(data._1)).map(e=>((DigestUtils.md5Hex(e._5),e._1,e._7,e._6),1)).
//          countByKey().foreach(e=>{
//          util.insert(insertSql,day,e._1._1,e._1._2,e._1._3,e._1._4,new JLong(e._2))
//        })

        DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.CRASH_LOG,inputDate).select(
          "MAC","STACK_TRACE","DATE_CODE","PRODUCT_CODE"
        ).map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3))).
          map(log => (log._1.replace(":",""),log._2,log._3,log._4))
          .filter(data => !DevMacUtils.macFilter(data._1)).filter(_._1!=null).filter(_._2!=null).filter(_._3!=null).
          filter(_._4!=null)
          .map(e=>((DigestUtils.md5Hex(e._2),e._1,e._4,e._3),1)).
          countByKey().foreach(e=>{
          util.insert(insertSql,day,e._1._1,e._1._2,e._1._3,e._1._4,new JLong(e._2))
        })



      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}