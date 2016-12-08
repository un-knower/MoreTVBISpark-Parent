package com.moretv.bi.report.medusa.productUpdateEvaluate.crash

/**
 * Created by Administrator on 2016/3/28.
 */

import java.lang.{Long => JLong}

import com.moretv.bi.medusa.util.DevMacUtils
import com.moretv.bi.medusa.util.ParquetDataStyle.ALL_CRASH_INFO
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.json.JSONObject

object CrashTrendsByVersionProductCrashKeyTemp extends SparkSetting{
  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  val util = new DBOperationUtils("medusa")

  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) =>{
        val inputDate = p.startDate
        val day = DateFormatUtils.toDateCN(inputDate)

        // 过滤掉stack_trace没有值/空的情形
        val logRdd = sc.textFile(s"/log/medusa_crash/rawlog/${inputDate}/").map(log=>{
          val json = new JSONObject(log)
          (json.optString("fileName"),json.optString("MAC"),json.optString("APP_VERSION_NAME"),json.optString("APP_VERSION_CODE"),
            json.optString("CRASH_KEY"),json.optString("STACK_TRACE"),json.optString("DATE_CODE"),json.optString("PRODUCT_CODE"))
        }).filter(e=>{e._6!=null && e._6!=""  && {if(e._7!=null) e._7.length<=20 else true}})

        // 内存溢出的crash
        val filterOutofMemoryRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._5.contains("java.lang." +
          "OutOfMemoryError"))
        // 空指针的crash
        val filterNullPointerRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log
          ._7,log._8)).filter(data => !DevMacUtils.macFilter(data._2)).filter(_._5.contains("java" +
          ".lang.NullPointerException"))
        // 所有的crash
        val filterAllRdd = logRdd.map(log => (log._1,log._2.replace(":",""),log._3,log._4,log._5,log._6,log._7,log
          ._8)).filter(data => !DevMacUtils.macFilter(data._2))

        /**
         * Transform the RDD to DataFrame
         */

        val DF2 = filterOutofMemoryRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF2.registerTempTable("crashInfo2")

        val DF5 = filterNullPointerRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,
          data._6,data._7,data._8)).toDF()
        DF5.registerTempTable("crashInfo5")

        val DFAll = filterAllRdd.map(data => ALL_CRASH_INFO(data._1,data._2,data._3,data._4,data._5,data._6,
          data._7,data._8)).toDF()
        DFAll.registerTempTable("crashInfoAll")

        if(p.deleteOld){
          val deleteSql = "delete from temp2 where day = ?"
          util.delete(deleteSql,day)
        }

        /*insert sql*/
        val sqlInsert = "insert into temp2(day,buildDate,product_code," +
          "app_version_name," +
          "crashType,total_number,total_user) VALUES(?,?,?,?,?,?,?)"
        /**
         * 统计不同的crashType和buildDate情况S
         */
        /*query sql*/
        // 统计各个apkVersion、BuildDate的所有 crash
        val sql6 = "select App_version_name,Date_code,'All' as Product_code,count(Mac),count(distinct " +
          "Mac) from crashInfoAll where App_version_name is not null and App_version_name != '' and length(App_version_name)=5" +
          " and Product_code not in ('EGREAT_S4_2.1','MYSOTO_S4_2.1','PULIER_S4_2.1'," +
          "'10moons_A10S','LeTVX60','FullAOSPonHaierAmber3','FullAOSPonKonkaAmber3','CEA1','Skyworth9R50G8210'," +
          "'FullAOSPonTclAmber3','FullAOSPonHisenseAmber3(NAND)','MeLEHTPC','Mele-M1','XT780'," +
          "'E50LX7100','BBA42','M1_T','L_Amlogic7366_V500HJ1-PE8_2D_CHI','L_Amlogic7366_LSC550HN01_2D_CHI'," +
          "'L_Amlogic7366_ST315A05-1_2D_CHI','EGREAT_V15','XT810','KonkaAndroidTV918','KIUI_MS9180_Y','K380','EC590'," +
          "'Skyworth8S6114A43','KIUI_MS9180_F','Skyworth8S2614U','Skyworth8S6114K','Skyworth8S6114KJ','Skyworth8S26U2'," +
          "'KIUI_MS9180','TV628','Yunhe-BT-4001','onkaAndroidTV638','KIUI_AW31S','FullAOSPongodbox','FlintStoneDigitalT2-S'," +
          "'MxS','K200_hdmiin','Q2S','H7','K200','B-200','B-303','FullAOSPonkonka_800C_DTMB','m201_512m'," +
          "'FullAOSPonkonka_800C','MiTV3','MiTV2S-48','A20','608p83','FullAOSPonMStarEagle','GenericAndroidonmt5396'," +
          "'KIUI6-M','LE42A5000','INPHIC_I10M','TV608','YK-K1','Skyworth8K55E680','TXCZ_A10TVBOX','608pb831','Box1'," +
          "'Skyworth8K56E380S','XMATE_A10S','I7_9_S12_D1G_F8G','KIUI6','m201','KonkaAndroidTV638'," +
          "'M8_SDK12_DDR1G','EGREAT_V2','8S61_14A43','B860A','Skyworth8K98E780U','rk312x','TclAndroidTV'," +
          "'ChanghongAndroidTV','letv_c1s','Mele_M1','C42S','SmartTV','INPHIC_I9','F1(X)','LetvU2','MYSOTO_A31S_S5'," +
          "'XMATE_A295','Softwinner','32PHF5021','INPHIC_I6','FullAOSPonTclAmber3','LeTVX60','FullAOSPonKonkaAmber3'," +
          "'EGREAT_S4_2.1','FullAOSPonHaierAmber3','FullAOSPonHisenseAmber3(NAND)')" +
          " group by App_version_name,Date_code"
        val rdd6 = sqlContext.sql(sql6).map(e=>(e.getString(0),e.getString
          (1),e.getString(2),e.getLong(3),e.getLong(4))).map(e=>(e._1,e._2,e._3,e._4,e._5)).collect()
        rdd6.foreach(r=>{
          util.insert(sqlInsert,day,r._2,r._3,r._1,"All",new JLong(r._4),new JLong(r._5))
        })

      }
      case None => {throw new RuntimeException("At least need one param: --startDate")}
    }
  }
}
