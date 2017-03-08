package com.moretv.bi.report.medusa.CrashLog

/**
 * Created by xiajun on 2016/9/21.
 * 该对象用于获取每天的crash的原始数据
 */
import java.lang.{Long => JLong}

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ParamsParseUtil, _}
import org.apache.commons.codec.digest.DigestUtils
import org.json.JSONObject


object EachUserEachCrashAppearInfo extends BaseClass{


  def main(args: Array[String]) {
    ModuleClass.executor(EachUserEachCrashAppearInfo,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val s = sqlContext
        import s.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate)
        // TODO 是否需要写到固定的常量类或者SDK读取
        val logRdd = sc.textFile(s"/log/medusa_crash/rawlog/${inputDate}/").map(log=>{
          val json = new JSONObject(log)
          (json.optString("fileName"),json.optString("MAC"),json.optString("APP_VERSION_NAME"),json.optString("APP_VERSION_CODE"),
            json.optString("ANDROID_VERSION"),json.optString("STACK_TRACE"),json.optString("DATE_CODE"),
            json.optString("PRODUCT_CODE"),json.optString("CUSTOM_JSON_DATA"))
        })
        val filterRdd = logRdd.repartition(28).map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log._7,log._8,log._9))
          .filter(data => !DevMacUtils.macFilter(data._2))

        /**
         * Create a new DF which including the md5
         */
        filterRdd.map(e=>(e._1,DigestUtils.md5Hex(e._1),e._2,e._3,e._4,e._5,e._6,DigestUtils.md5Hex(e._6),e._7,e._8,e._9)).
          toDF("fileName","fileName_md5","mac","appVersionName","appVersionCode","androidVersion","stackTrace","stackTraceMD5",
          "dateCode","productCode","customJsonData").registerTempTable("crashInfo_MD5")

        /**
         * The statistic process for different needs
         *
         */

        val rdd = sqlContext.sql("select stackTraceMD5,mac,productCode,dateCode," +
          "count(fileName_md5) " +
          "from crashInfo_MD5 group by stackTraceMD5,mac,productCode,dateCode").
          map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getLong(4)))

        val insertSql = "insert into tmp_crash_info(day,mac,stackTraceMD5,productCode,dateCode,num) values(?,?,?,?,?,?)"

        if(p.deleteOld){
          val deleteSql = "delete from tmp_crash_info where day = ?"
          util.delete(deleteSql,day)
        }
        rdd.collect().foreach(r=>{
          try{
            util.insert(insertSql,day,r._2,r._1,r._3,r._4,new JLong(r._5))
          }catch{
            case e =>
          }
        })
      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}

