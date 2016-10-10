package com.moretv.bi.temp

import java.sql.DriverManager
import java.util.regex.Pattern

import com.moretv.bi.util.{UserIdUtils, DateFormatUtils, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Will on 2015/2/5.
 */
object YunOSNewUser extends SparkSetting{

  val pattern = Pattern.compile("\\[(\\d{2}/[a-zA-Z]{3}/\\d{4}).+?/login/Service/(login|enlogin|logon)\\?.?" +
    "mac=([a-zA-Z0-9]{12}).+?ProductModel=(\\w+)")

  val productModelArray1 = Array("MagicBox1s_Plus",
    "MagicBox1s_Pro",
    "magicbox",
    "MagicBox2",
    "MagicBox1s",
    "MagicBox_M11",
    "MagicBox_M11_MEIZU",
    "MagicBox_beta")

  val productModelArray2 = Array("KIUI6-M",
    "KBE_3066",
    "KBE_3128M",
    "KBE_H8",
    "KIUI",
    "KBE_AW31S",
    "KBE_3188",
    "KBE_T3",
    "KIUI-3188",
    "KBE_3188D",
    "KBE_K610I",
    "KBE_A20M",
    "H7",
    "KIUI_AW31S_M",
    "KIUI6",
    "KIUI_AW31S",
    "KIUI_MS9180_F",
    "KIUI_3128_FG",
    "KBE_AW31S_M",
    "KIUI_Q2",
    "10MOONS_D6Q",
    "10MOONS_ELF5",
    "10moons_A20",
    "10MOONS_ELF6",
    "10MOONS_D6",
    "10MOONS_LT390WD",
    "10MOONS_D6U",
    "10moons_D8G",
    "10MOONS_T2Q",
    "10MOONS_ELF3",
    "Yunhe-BT-4001",
    "10MOONS_D9",
    "10moons_A10S",
    "10MOONS_D8",
    "INPHIC_I9E",
    "INPHIC_I9H",
    "INPHIC_I9",
    "INPHIC_I10M",
    "INPHIC_I6H",
    "INPHIC_I7S",
    "INPHIC_I10S",
    "INPHIC_I9S",
    "INPHIC_I6X",
    "INPHIC_H3",
    "INPHIC_I6",
    "XMATE_R31",
    "XMATE_A29",
    "DIYOMATE_A20",
    "XMATE_A20",
    "A10s-TVBOX",
    "XMATE_A293",
    "XMATE_A10S",
    "XMATE_A294",
    "XMATE_A88",
    "XMATE_A292",
    "XMATE_A295",
    "IDER_BBA41",
    "BBA22",
    "IDER_BBA22",
    "IDER_BBA32",
    "IDER_BBA43",
    "IDER_BBA23",
    "IDER_BBA31",
    "NINSS_BBA42BTCXZ",
    "NINSS_BBA22O",
    "IDER_BBA33",
    "EGREAT_S4_2.1",
    "EGREAT_V15",
    "MeleHTPC",
    "Mele-HTPC",
    "A100",
    "TXCZ_R10B",
    "LY_3128Q3",
    "CX_A19")

  val productModelArray3 = Array("AndroidTVonHaier6A600",
    "HaierAndroidTV",
    "FullAOSPonHaierAmber3",
    "LE42A5000",
    "m201",
    "AndroidTVonHaier6A600hd",
    "m201_512m",
    "KKTV_K43")

  def main(args: Array[String]) {

    config.setAppName("YunOSNewUser").
      set("spark.executor.memory", "2g").
      set("spark.cores.max", "50").
      set("spark.storage.memoryFraction", "0.6")

    val sc = new SparkContext(config)

    val sqlRDD = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
        "bi", "mlw321@moretv")
    },
      "SELECT left(openTime,10),product_model,mac FROM `mtv_account` WHERE ID >= ? AND ID <= ? and left(openTime,10) >= '2015-10-01'",
      230066291,
      254145551,
      60,
      r=>(r.getString(1),r.getString(2),r.getString(3))).map(t => if(matchLog(t._2)) (t._1,t._3) else null).
        filter(_ != null).distinct()


    val user = sqlRDD.groupByKey().map(x => (x._1,x._2.size)).collect().sortBy(_._1)

    sc.stop()

    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    println("accessUser:")
    user.foreach(x => {
      println(x._1 + "," + x._2)
    })
    println("#########################################")
  }

  def matchLog(productModel:String) = {
    productModelArray1.exists(p => {
      p.equalsIgnoreCase(productModel)
    })
  }

}
