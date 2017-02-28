package com.moretv.bi.report.medusa.CrashLog

/**
 * Created by Administrator on 2016/3/30.
  *
  * not used
 */

//import java.text.SimpleDateFormat
import java.lang.{Long => JLong}
import java.sql.DriverManager

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
//import java.util.Date
//import java.util.Calendar
import org.apache.spark.storage.StorageLevel

object CrashMetaInfo2 extends SparkSetting{
  def main(args: Array[String]) {
    // Getting today!!!!
//    val today = new Date()
//    val calendar = Calendar.getInstance()
//    calendar.setTime(today)
//    calendar.add(Calendar.DAY_OF_MONTH,0)
//    val cnFormat = new SimpleDateFormat("yyyy-MM-dd")
//    val update_day = cnFormat.format(calendar.getTime)

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val input = p.startDate
        val inputDay = DateFormatUtils.toDateCN(input)
        val sc = new SparkContext(config)
//        val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        /**
         * Define two lambda functions
         */
        val minId = (util: MySqlOps,inputDay:String,flag:Boolean) => {
          flag match {
            case true=>{
              val sql = "SELECT Min(id) from medusa_crash_meta_info"
              val arr = util.selectOne(sql)
              arr(0).toString.toLong
            }
            case false=>{
              val sql = s"SELECT Min(id) from medusa_crash_original_info WHERE day= '$inputDay'"
              val arr = util.selectOne(sql)
              arr(0).toString.toLong
            }
          }
        }
        val maxId = (util: MySqlOps,inputDay:String,flag:Boolean) => {
          flag match {
            case true=>{
              val sql = "SELECT MAX(id) FROM medusa_crash_meta_info"
              val arr = util.selectOne(sql)
              arr(0).toString.toLong
            }
            case false=>{
              val sql = s"SELECT MAX(id) from medusa_crash_original_info where day='$inputDay'"
              val arr = util.selectOne(sql)
              arr(0).toString.toLong
            }
          }
        }

        val numOfPartition = 20

        /**
         * Getting the data from original table, which includes the information of each day
         */
        println("==================================")
        println(minId(util,inputDay,false))
        println(maxId(util,inputDay,false))
        println("==================================")

        val jdbc_original_rdd = new JdbcRDD(sc,
          () => {
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
          },
          "SELECT day, app_version_name, app_version_code, package, crash_key, crash_key_md5,stack_trace, stack_trace_md5," +
            "crash_num from medusa_crash_original_info where id >=? and id <= ? ",
          minId(util,inputDay,false),
          maxId(util,inputDay,false),
          numOfPartition,
          r => (r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), r.getString(6), r.getString
            (7), r.getString(8), r.getLong(9))
        )

        /**
         * Getting the info from the meta table
         */
        println("==================================")
        println(minId(util,inputDay,true))
        println(maxId(util,inputDay,true))
        println("==================================")
        val jdbc_meta_rdd = new JdbcRDD(sc,
          () =>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
          },
          "SELECT create_day, update_day, app_version_name, app_version_code, package, crash_key, " +
            "crash_key_md5, stack_trace, stack_trace_md5, crash_num, is_resolved, is_reappear FROM medusa_crash_meta_info " +
            "where id >= ? and id<=?",
          minId(util,inputDay,true),
          maxId(util,inputDay,true),
          numOfPartition,
          r=>(r.getString(1),r.getString(2),r.getString(3),r.getString(4),r.getString(5),r.getString(6),r.getString(7),r
            .getString(8),r.getString(9),r.getLong(10),r.getLong(11),r.getLong(12))
        )


        /**
         * If the meta_rdd is null, then put the original_info into meta_rdd
         * If the meta_rdd has data, then calculate the new info
         */

        val tempRDD = jdbc_original_rdd.map(e=>(e._1,inputDay,e._2,e._3,e._4,e._5,e._6,e._7,e._8,e._9,0.toLong,0
          .toLong)).persist(StorageLevel.DISK_ONLY)

        println("==================Having get the meta info=========================")
        println("The length of meta is: "+jdbc_meta_rdd.collect().length)
//        jdbc_meta_rdd.collect().foreach(e=>{
//          println(e._1+" "+e._2+" "+e._3+" "+e._4+" "+e._5+" "+e._6+" "+e._7+" "+e._8+" "+e._9+" "+e._10+" "+e._11+" "+e._12)
//        })
//        if (jdbc_meta_rdd.isEmpty()) {
//          val merge_rdd = jdbc_meta_rdd.++(tempRDD)
//          val insert_sql = "INSERT INTO medusa_crash_meta_info(create_day, update_day, app_version_name, app_version_code," +
//            "package, crash_key,crash_key_md5, stack_trace, stack_trace_md5, crash_num, is_resolved, is_reappear)"
//          merge_rdd.foreach(e=>{
//            util.insert(insert_sql,e._1,e._2,e._3,e._4,e._5,e._6,e._7,e._8,e._9,new JLong(e._10),new JLong(e
//              ._11),new JLong(e._12))
//          })
//        }else {


        val insertNew = "INSERT INTO medusa_crash_meta_info(create_day,update_day,app_version_name,app_version_code," +
          "package,crash_key,crash_key_md5,stack_trace,stack_trace_md5,crash_num,is_resolved,is_reappear) VALUES(?,?," +
          "?,?,?,?,?,?,?,?,?,?)"




        /**
         * @1:the new crash has not appear in the meta_info table
         */
        println("================Begin insert new crash into meta_info table=================")
        val version_key_trace_list = jdbc_meta_rdd.map(e=>(e._3,e._7,e._9)).collect()
        println("Total version_key_trace_list is: "+version_key_trace_list.length)
        val new_appear_info = tempRDD.filter(e => !version_key_trace_list.contains((e._3,e._7,e._9))).collect()
        println("New crash num is:" + new_appear_info.length)
        new_appear_info.foreach(e=>{
          util.insert(insertNew,e._1, e._2,e._3,e._4,e._5,e._6,e._7,e._8,e._9,new JLong(e._10),new JLong(e
            ._11), new JLong(e._12))
        })


          /**
           * Deal with the case: crash has not been resolved!
           * @2: the new crash has appear in the meta_info table
           */
        println("=============Begin merge old data and new data=========================")
          val un_resolved_rdd = jdbc_meta_rdd.filter(_._11==0).filter(_._1<inputDay).cache()
          val merge_info = un_resolved_rdd.map(e=>((e._3,e._7,e._9),(e._1,e._2,e._4,e._5,e._6,e._8,e._10,e._11,e
            ._12))) join tempRDD.map(e=>((e._3,e._7,e._9),e._10))
          val new_merge_info = merge_info.map(e=>((e._1._1,e._1._2,e._1._3),(e._2._1._1,inputDay,e._2._1._3,e._2._1._4,e
            ._2._1._5,e._2._1._6,e._2._1._7+e._2._2,e._2._1._8,e._2._1._9))).collect()
          println("The crash has not resolved is: "+new_merge_info.length)
          new_merge_info.foreach(e=>{
            val updateSql = s"UPDATE medusa_crash_meta_info SET update_day=?, crash_num = ? WHERE app_version_name='${e._1
              ._1}' " +
              s"and " +
              s"crash_key_md5='${e._1._2}' and stack_trace_md5='${e._1._3}'"
            util.update(updateSql,e._2._2,new JLong(e._2._7))
          })

//          new_merge_info.foreach(e=>{
//            //Update the database by delete the old data firstly and insert the new data
//            val deleteOld = s"delete from medusa_crash_meta_info where app_version_name='${e._1._1}' and crash_key_md5='${e
//              ._1._2}' and stack_trace_md5='${e._1._3}'"
//            util.delete(deleteOld)
//            util.insert(insertNew,e._2._1,e._2._2,e._1._1,e._2._3,e._2._4,e._2._5,e._1._2,e._2._6,e._1._3,new
//                JLong(e._2._7),new JLong(e._2._8), new JLong(e._2._9))
//          })



          /**
           * Deal with the case: the crash have been resolved
           *@3:the new crash is the reappear
           * Need considering the app_version_name is higher and the crash_num > 20
           */
          println("=========================Begin statistic reappear data===========================")
          val resolved_rdd = jdbc_meta_rdd.filter(_._11==1).cache()
          if (resolved_rdd.collect().length>0){
            val reappear_merge_info = resolved_rdd.map(e=>((e._7,e._9),(e._1,e._2,e._4,e._5,e._6,e._8,e._10,e._11,e
              ._12,e._3))) join tempRDD.map(e=>((e._7,e._9),(e._10,e._3)))
            val reappear_new_merge_info = reappear_merge_info.map(e=>(e._2._2._2>e._2._1._10 & (e._2._1._7+e._2._2._1)>20)
            match {
              case true=>(e._2._1._1,e._2._1._2,e._2._1._10,e._2._1._3,e._2._1._4,e._2._1._5,e._1._1,e._2._1._6,e._1._2,e._2._1
                ._7+e._2._2
                ._1, 0.toLong, e._2._1._9+1)
              case false=>(e._2._1._1,e._2._1._2,e._2._1._10,e._2._1._3,e._2._1._4,e._2._1._5,e._1._1,e._2._1._6,e._1._2,e._2._1._7+e._2._2
                ._1, e._2._1._8, e._2._1._9)
            }).collect()

            reappear_new_merge_info.foreach(e=>{
              val updateSql = s"UPDATE medusa_crash_meta_info SET update_day=?,crash_num = ?,is_resolved = ?,is_reappear = ?" +
                s" WHERE " +
                s"app_version_name='${e._3}' and " +
                s"crash_key_md5='${e._7}' and stack_trace_md5='${e._9}'"
              util.update(updateSql,e._2,new JLong(e._10),new JLong(e._11),new JLong(e._12))
              //Update the database by delete the old data firstly and insert the new data
              //            val deleteOld = s"delete from medusa_crash_meta_info where app_version_name='${e._3}' and crash_key_md5='${e
              //              ._7}' and stack_trace_md5='${e._9}'"
              //            util.delete(deleteOld)
              //            util.insert(insertNew,e._1, e._2, e._3, e._4, e._5, e._6, e._7, e._8, e._9, new JLong(e._10),new JLong(e._11),
              //              new JLong(e._12))
            })
          }
//        }

        tempRDD.unpersist()
      }
      case None => {
        throw new RuntimeException("Needs the param: startDate!")
      }
    }
  }
}
