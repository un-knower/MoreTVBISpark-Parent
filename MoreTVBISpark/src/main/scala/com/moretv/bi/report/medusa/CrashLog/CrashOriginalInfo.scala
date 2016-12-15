package src.com.moretv.bi.report.medusa.CrashLog

/**
 * Created by xiajun on 2016/3/28.
 * 该对象用于获取每天的crash的原始数据
 */
import java.lang.{Long => JLong}

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{ParamsParseUtil, _}
import org.apache.commons.codec.digest.DigestUtils
import org.json.JSONObject


object CrashOriginalInfo extends BaseClass{


  def main(args: Array[String]) {
    ModuleClass.executor(CrashOriginalInfo,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val s = sqlContext
        import s.implicits._
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate)
        val logRdd = sc.textFile(s"/log/medusa_crash/rawlog/${inputDate}/").map(log=>{
          val json = new JSONObject(log)
          (json.optString("fileName"),json.optString("MAC"),json.optString("APP_VERSION_NAME"),json.optString("APP_VERSION_CODE"),
            json.optString("ANDROID_VERSION"),json.optString("STACK_TRACE"),json.optString("DATE_CODE"),json.optString("PRODUCT_CODE"))
        })

        val filterRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log._7,log._8))
          .filter(data => !DevMacUtils.macFilter(data._2))

        /**
         * Create a new DF which including the md5
         */
        filterRdd.map(e=>(e._1,DigestUtils.md5Hex(e._1),e._2,e._3,e._4,e._5,e._6,DigestUtils.md5Hex(e._6),e._7,e._8)).
          toDF("fileName","fileName_md5","mac","appVersionName","appVersionCode","androidVersion","stackTrace","stackTraceMD5",
          "dateCode","productCode").registerTempTable("crashInfo_MD5")

        /**
         * The statistic process for different needs
         *
         */

        val rdd = sqlContext.sql("select appVersionName, androidVersion,dateCode,productCode,stackTraceMD5,stackTrace from" +
          " crashInfo_MD5").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),e.getString(5)))

        // Getting the number of each version/crash_key/stack_trace
        val crashNumRdd = sqlContext.sql("select appVersionName, androidVersion,dateCode,productCode,stackTraceMD5," +
          "stackTrace,count(fileName_md5) from crashInfo_MD5 group by appVersionName, androidVersion,dateCode,productCode," +
          "stackTraceMD5,stackTrace").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3),e.getString(4),
          e.getString(5),e.getLong(6)))

        val merger = rdd.distinct().map(e=>((e._1,e._2,e._3,e._4,e._5,e._6),1.toLong)) join(crashNumRdd.map(e=>((e._1,e._2,e
          ._3,e._4,e._5,e._6),(e._7))))

        val metaData = merger.map(e=>(e._1._1, e._1._2, e._1._3, e._1._4, e._1._5,e._1._6,e._2._2)).distinct()
          .collect()
        // Insert data into database
        val insert_sql = "INSERT INTO medusa_crash_original_secondary_phase_info(day,app_version_name,android_version," +
          "date_code,product_code,stack_trace_md5,stack_trace,crash_num) VALUES(?,?,?,?,?,?,?,?)"
        if(p.deleteOld) {
          val date = DateFormatUtils.toDateCN(p.startDate, -1)
          val oldSql = s"delete from medusa_crash_original_secondary_phase_info where day = '$date'"
          util.delete(oldSql)
        }
        metaData.foreach(e=>{
          try{
            util.insert(insert_sql,day,e._1,e._2,e._3,e._4,e._5,e._6,new JLong(e._7))
          }catch{
            case e:Exception=> e.printStackTrace()
          }

        })

        rdd.unpersist()
      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}

