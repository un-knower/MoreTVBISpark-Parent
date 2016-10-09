package com.moretv.bi.temp.user

import java.text.SimpleDateFormat
import java.util.Calendar

import com.moretv.bi.util.SparkSetting
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._
import com.moretv.bi.util.FileUtils._

/**
  * Created by Will on 2016/7/5.
  */
object PingbiProductModel extends SparkSetting{

  private val pmList = List("EGREAT_S4_2.1",
    "MYSOTO_S4_2.1",
    "PULIER_S4_2.1",
    "10moons_A10S",
    "LeTVX60",
    "FullAOSPonHaierAmber3",
    "FullAOSPonKonkaAmber3",
    "CEA1",
    "Skyworth9R50G8210",
    "FullAOSPonTclAmber3",
    "FullAOSPonHisenseAmber3(NAND)",
    "MeLEHTPC",
    "Mele-M1",
    "XT780",
    "E50LX7100",
    "BBA42",
    "M1_T",
    "L_Amlogic7366_V500HJ1-PE8_2D_CHI",
    "L_Amlogic7366_LSC550HN01_2D_CHI",
    "L_Amlogic7366_ST315A05-1_2D_CHI",
    "EGREAT_V15",
    "XT810",
    "KonkaAndroidTV918",
    "KIUI_MS9180_Y",
    "K380",
    "EC590",
    "Skyworth8S6114A43",
    "KIUI_MS9180_F",
    "Skyworth8S2614U",
    "Skyworth8S6114K",
    "Skyworth8S6114KJ",
    "Skyworth8S26U2",
    "KIUI_MS9180",
    "TV628",
    "Yunhe-BT-4001",
    "onkaAndroidTV638",
    "KIUI_AW31S",
    "FullAOSPongodbox",
    "FlintStoneDigitalT2-S",
    "MxS",
    "K200_hdmiin",
    "Q2S",
    "H7",
    "K200",
    "B-200",
    "B-303",
    "FullAOSPonkonka_800C_DTMB",
    "m201_512m",
    "FullAOSPonkonka_800C",
    "MiTV3",
    "MiTV2S-48",
    "A20",
    "608p83",
    "FullAOSPonMStarEagle",
    "GenericAndroidonmt5396",
    "KIUI6-M",
    "LE42A5000",
    "INPHIC_I10M",
    "TV608",
    "YK-K1",
    "Skyworth8K55E680",
    "TXCZ_A10TVBOX",
    "608pb831",
    "Box1",
    "Skyworth8K56E380S",
    "XMATE_A10S",
    "I7_9_S12_D1G_F8G",
    "KIUI6",
    "m201",
    "KonkaAndroidTV638",
    "M8_SDK12_DDR1G",
    "EGREAT_V2",
    "8S61_14A43",
    "B860A",
    "Skyworth8K98E780U",
    "rk312x",
    "TclAndroidTV",
    "ChanghongAndroidTV",
    "letv_c1s",
    "Mele_M1",
    "C42S",
    "SmartTV",
    "INPHIC_I9",
    "F1(X)",
    "LetvU2",
    "MYSOTO_A31S_S5",
    "XMATE_A295",
    "Softwinner",
    "32PHF5021",
    "INPHIC_I6")
  def main(args: Array[String]) {
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.read.load("/log/dbsnapshot/parquet/20160731/moretv_mtv_account").registerTempTable("log_data")
    val pms = "'"+pmList.mkString("','")+"'"
    sqlContext.read.load("/log/moretvloginlog/parquet/20160{7*,801}/loginlog").registerTempTable("log_login")
    sqlContext.read.parquet("/log/medusaAndMoretvMerger/20160{7*,801}/playview").registerTempTable("log_play")
    sqlContext.udf.register("leftsub",(x:String,y:Int) => x.substring(0,y))
    //屏蔽终端总用户数统计
     val totalUsers = sqlContext.sql(s"select product_model,count(distinct mac) from log_data where" +
      s" openTime <= '2016-07-31 23:59:59' and product_model in ($pms) group by product_model").collectAsList()
    //屏蔽终端日新增统计
    val newUsers = sqlContext.sql(s"select leftsub(openTime,10),product_model,count(distinct mac) from log_data " +
      s"where openTime >= '2016-07-01 00:00:00' and openTime <= '2016-07-31 23:59:59' and product_model in ($pms) " +
      s"group by leftsub(openTime,10),product_model").collectAsList().
      map(row => ((row.getString(0),row.getString(1)),row.getLong(2))).toMap
    val loginUsers = sqlContext.sql(s"select date,productModel,count(distinct userId) from log_login " +
      s"where date >= '2016-07-01' and date <= '2016-07-31' and productModel in ($pms) group by date,productModel").
      collectAsList().map(row => ((row.getString(0),row.getString(1)),row.getLong(2))).toMap
    val playVVs = sqlContext.sql(s"select date,productModel,count(userId)/count(distinct userId) from log_play " +
      s"where date >= '2016-07-01' and date <= '2016-07-31' and event in ('startplay','playview') and " +
      s"productModel in ($pms) group by date,productModel").collectAsList().
      map(row => ((row.getString(0),row.getString(1)),row.getDouble(2))).toMap
    val playDuration = sqlContext.sql(s"select date,productModel,sum(duration)/count(distinct userId) from log_play " +
      s"where date >= '2016-07-01' and date <= '2016-07-31' and event <> 'startplay' " +
      s"and duration between 0 and 10800 and productModel in ($pms) group by date,productModel").collectAsList().
      map(row => ((row.getString(0),row.getString(1)),row.getDouble(2))).toMap

    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.set(2016,6,0)
    val days = for(i <- 0 to 30) yield {
      cal.add(Calendar.DAY_OF_MONTH,1)
      format.format(cal.getTime)
    }
    withCsvWriterOld("/script/bi/moretv/liankai/file/PingbiProductModel-pc-total-20160806.csv"){
      out => {
        totalUsers.foreach(e => out.println(e.getString(0)+","+e.getLong(1)))
      }
    }

    withCsvWriterOld("/script/bi/moretv/liankai/file/PingbiProductModel-pc-20160806.csv"){
      out => {
        days.foreach(day => {
          pmList.foreach(pm => {
            val key = (day,pm)
            val newUser = newUsers.getOrElse(key,0L)
            val loginUser = loginUsers.getOrElse(key,0L)
            val playVV= playVVs.getOrElse(key,0.0)
            val duration = playDuration.getOrElse(key,0.0)
            out.println(s"$day,$pm,$newUser,$loginUser,${loginUser - newUser},$playVV,$duration")
          })
        })
      }
    }
  }

}
