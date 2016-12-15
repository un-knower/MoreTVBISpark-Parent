package com.moretv.bi.report.medusa.productUpdateEvaluate.crash

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/5/16.
 * 该对象用于统计一周的信息
 * 播放率对比：播放率=播放人数/活跃人数
 */
object MedusaDailyActivityInfoTemp extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val sc = new SparkContext(config)
        implicit val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate

        val medusaDir = "/log/medusa/parquet"


        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val medusaDailyActiveInput = s"$medusaDir/$date/*/"
          val medusaDailyEnterInput = s"$medusaDir/$date/enter/"

          sqlContext.read.parquet(medusaDailyActiveInput).select("userId","apkVersion","buildDate","productModel").
            filter("length(buildDate)<20").registerTempTable("all_log")
          sqlContext.read.parquet(medusaDailyEnterInput).select("userId","apkVersion","buildDate","productModel").
            filter("length(buildDate)<20").registerTempTable("enter_log")


          if(p.deleteOld){
            val deleteSql = "delete from temp1 where day=?"
            util.delete(deleteSql,insertDate)
          }

          val sqlInsert = "insert into temp1(day,apk_version," +
            "buildDate,productModel,active_user,active_num) values (?,?,?,?,?,?)"

          // 计算每个apkVersion、不同buildDate所有productModel的日活登录次数
          val userRdd2=sqlContext.sql("select apkVersion,buildDate,'All' as productModel,count(distinct userId) from " +
            "all_log where length(apkVersion)=5 and productModel not in ('EGREAT_S4_2.1','MYSOTO_S4_2.1','PULIER_S4_2.1'," +
            "'10moons_A10S','LeTVX60','FullAOSPonHaierAmber3','FullAOSPonKonkaAmber3','CEA1','Skyworth9R50G8210'," +
            "'FullAOSPonTclAmber3','FullAOSPonHisenseAmber3(NAND)','MeLEHTPC','Mele-M1','XT780','E50LX7100','BBA42'," +
            "'M1_T','L_Amlogic7366_V500HJ1-PE8_2D_CHI','L_Amlogic7366_LSC550HN01_2D_CHI','L_Amlogic7366_ST315A05-1_2D_CHI'," +
            "'EGREAT_V15','XT810','KonkaAndroidTV918','KIUI_MS9180_Y','K380','EC590','Skyworth8S6114A43','KIUI_MS9180_F'," +
            "'Skyworth8S2614U','Skyworth8S6114K','Skyworth8S6114KJ','Skyworth8S26U2','KIUI_MS9180','TV628','Yunhe-BT-4001'," +
            "'onkaAndroidTV638','KIUI_AW31S','FullAOSPongodbox','FlintStoneDigitalT2-S','MxS','K200_hdmiin','Q2S','H7'," +
            "'K200','B-200','B-303','FullAOSPonkonka_800C_DTMB','m201_512m','FullAOSPonkonka_800C','MiTV3','MiTV2S-48'," +
            "'A20','608p83','FullAOSPonMStarEagle','GenericAndroidonmt5396','KIUI6-M','LE42A5000','INPHIC_I10M','TV608'," +
            "'YK-K1','Skyworth8K55E680','TXCZ_A10TVBOX','608pb831','Box1','Skyworth8K56E380S','XMATE_A10S'," +
            "'I7_9_S12_D1G_F8G','KIUI6','m201','KonkaAndroidTV638','M8_SDK12_DDR1G','EGREAT_V2','8S61_14A43','B860A'," +
            "'Skyworth8K98E780U','rk312x','TclAndroidTV','ChanghongAndroidTV','letv_c1s','Mele_M1','C42S','SmartTV'," +
            "'INPHIC_I9','F1(X)','LetvU2','MYSOTO_A31S_S5','XMATE_A295','Softwinner','32PHF5021','INPHIC_I6'," +
            "'FullAOSPonTclAmber3','LeTVX60','FullAOSPonKonkaAmber3','EGREAT_S4_2.1','FullAOSPonHaierAmber3'," +
            "'FullAOSPonHisenseAmber3(NAND)') group by apkVersion,buildDate").map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val numRdd2 = sqlContext.sql("select apkVersion,buildDate,'All' as productModel,count(userId) from enter_log " +
            "where length (apkVersion)=5 and productModel not in ('EGREAT_S4_2.1','MYSOTO_S4_2.1','PULIER_S4_2.1'," +
            "'10moons_A10S','LeTVX60','FullAOSPonHaierAmber3','FullAOSPonKonkaAmber3','CEA1','Skyworth9R50G8210'," +
            "'FullAOSPonTclAmber3','FullAOSPonHisenseAmber3(NAND)','MeLEHTPC','Mele-M1','XT780','E50LX7100','BBA42'," +
            "'M1_T','L_Amlogic7366_V500HJ1-PE8_2D_CHI','L_Amlogic7366_LSC550HN01_2D_CHI','L_Amlogic7366_ST315A05-1_2D_CHI'," +
            "'EGREAT_V15','XT810','KonkaAndroidTV918','KIUI_MS9180_Y','K380','EC590','Skyworth8S6114A43','KIUI_MS9180_F'," +
            "'Skyworth8S2614U','Skyworth8S6114K','Skyworth8S6114KJ','Skyworth8S26U2','KIUI_MS9180','TV628','Yunhe-BT-4001'," +
            "'onkaAndroidTV638','KIUI_AW31S','FullAOSPongodbox','FlintStoneDigitalT2-S','MxS','K200_hdmiin','Q2S','H7'," +
            "'K200','B-200','B-303','FullAOSPonkonka_800C_DTMB','m201_512m','FullAOSPonkonka_800C','MiTV3','MiTV2S-48'," +
            "'A20','608p83','FullAOSPonMStarEagle','GenericAndroidonmt5396','KIUI6-M','LE42A5000','INPHIC_I10M','TV608'," +
            "'YK-K1','Skyworth8K55E680','TXCZ_A10TVBOX','608pb831','Box1','Skyworth8K56E380S','XMATE_A10S'," +
            "'I7_9_S12_D1G_F8G','KIUI6','m201','KonkaAndroidTV638','M8_SDK12_DDR1G','EGREAT_V2','8S61_14A43','B860A'," +
            "'Skyworth8K98E780U','rk312x','TclAndroidTV','ChanghongAndroidTV','letv_c1s','Mele_M1','C42S','SmartTV'," +
            "'INPHIC_I9','F1(X)','LetvU2','MYSOTO_A31S_S5','XMATE_A295','Softwinner','32PHF5021','INPHIC_I6'," +
            "'FullAOSPonTclAmber3','LeTVX60','FullAOSPonKonkaAmber3','EGREAT_S4_2.1','FullAOSPonHaierAmber3'," +
            "'FullAOSPonHisenseAmber3(NAND)') group by apkVersion,buildDate").map(e=>(e.getString(0),e.getString
            (1),e.getString(2),e.getLong(3))).map(e=>((e._1,e._2,e._3),e._4))
          val rdd2 = userRdd2 join numRdd2
          rdd2.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,e._1._1,e._1._2,e._1._3,new JLong(e._2._1), new JLong(e._2._2))
          })



        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }
}
