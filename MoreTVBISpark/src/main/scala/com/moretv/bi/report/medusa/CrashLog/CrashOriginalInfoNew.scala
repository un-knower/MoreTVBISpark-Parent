package com.moretv.bi.report.medusa.CrashLog

/**
 * Created by xiajun on 2016/9/21.
 * 该对象用于获取每天的crash的原始数据
  *
  * this class is used
 */
import java.lang.{Long => JLong}

import com.moretv.bi.medusa.util.DevMacUtils
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{ParamsParseUtil, _}
import org.apache.commons.codec.digest.DigestUtils
import org.json.JSONObject


object CrashOriginalInfoNew extends BaseClass{


  def main(args: Array[String]) {
    config.set("spark.executor.memory", "5g").
      set("spark.executor.cores", "5").
      set("spark.cores.max", "100")
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val s = sqlContext
        import s.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate)
        val inputPath=p.paramMap.getOrElse("inputPath",s"/log/medusa_crash/rawlog/#{date}/").replace("#{date}",inputDate)
        val logRdd = sc.textFile(inputPath).map(log=>{
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


        val rdd = sqlContext.sql("select appVersionName, androidVersion,dateCode,productCode,stackTraceMD5,stackTrace," +
          "customJsonData,mac from crashInfo_MD5").map(e=>(e.getString(0),e.getString(1),e.getString(2),
          e.getString(3), e.getString(4),e.getString(5),e.getString(6),e.getString(7)))
//         Getting the number of each version/crash_key/stack_trace
        val crashNumRdd = sqlContext.sql("select appVersionName, androidVersion,dateCode,productCode,stackTraceMD5," +
          "stackTrace,customJsonData,count(fileName_md5) from crashInfo_MD5 group by appVersionName, " +
          "androidVersion,dateCode,productCode,stackTraceMD5,stackTrace,customJsonData").
          map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),e.getString(5),
          e.getString(6),e.getLong(7)))

        if(p.deleteOld) {
          val oldSql = s"delete from medusa_crash_original_info_new where day = '$day'"
          util.delete(oldSql)
        }

        val merger = rdd.distinct().map(e=>((e._1,e._2,e._3,e._4,e._5,e._6,e._7),1.toLong)) join(crashNumRdd.map(e=>((e._1,e._2,e
          ._3,e._4,e._5,e._6,e._7),(e._8))))

        val metaData = merger.map(e=>(e._1._1, e._1._2, e._1._3, e._1._4, e._1._5,e._1._6,e._1._7,
          e._2._2)).distinct()
        // Insert data into database
        val insert_sql = "INSERT INTO medusa_crash_original_info_new(day,app_version_name,android_version," +
          "date_code,product_code,stack_trace_md5,stack_trace,custom_json_data,crash_num) VALUES(?,?,?,?,?,?,?,?,?)"
        metaData.foreachPartition(rdd=>{
          val utilInsert = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
          rdd.foreach(e=>{
            try{
              utilInsert.insert(insert_sql,day,e._1,e._2,e._3,e._4,e._5,e._6,e._7,new JLong(e._8))
            }catch{
              case e:Exception=> e.printStackTrace()
            }
          })
        })

        rdd.unpersist()
      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}

