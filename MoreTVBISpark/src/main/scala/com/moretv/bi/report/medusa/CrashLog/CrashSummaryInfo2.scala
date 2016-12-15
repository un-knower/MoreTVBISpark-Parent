package com.moretv.bi.report.medusa.CrashLog

/**
 * Created by Administrator on 2016/4/6.
 */

import java.lang.{Long => JLong}
import java.sql.DriverManager

import com.moretv.bi.util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.storage.StorageLevel

object CrashSummaryInfo2 extends SparkSetting{
  def main(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val input = p.startDate
        val inputDay = DateFormatUtils.toDateCN(input)
        val sc = new SparkContext(config)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)


        /**
         * Define two lambda functions
         */
        val minId = (util: DBOperationUtils,inputDay:String,flag:Boolean) => {
          flag match {
            case true=>{
              val sql = "SELECT Min(id) from medusa_crash_summary_info"
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
        val maxId = (util: DBOperationUtils,inputDay:String,flag:Boolean) => {
          flag match {
            case true=>{
              val sql = "SELECT MAX(id) FROM medusa_crash_summary_info"
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

        val jdbc_original_from_db_rdd = new JdbcRDD(sc,
          () => {
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
          },
          "SELECT crash_key, crash_key_md5, crash_num,app_version_name from medusa_crash_original_info where id " +
            ">=? and id <= ?",
          minId(util,inputDay,false),
          maxId(util,inputDay,false),
          numOfPartition,
          r => (r.getString(1), r.getString(2), r.getLong(3),r.getString(4))
        )
        /**
         * Calculating the sum_crash_num of each crash_key
         * 需要过滤乱码的问题
         */
//        val jdbc_original_rdd = jdbc_original_from_db_rdd.filter(e=>FilterUnicodeString.filterUnicodeString(e._1))
        val jdbc_original_rdd = jdbc_original_from_db_rdd
        val jdbc_original_sum_num_rdd = jdbc_original_rdd.map(e=>(e._2,e._3)).reduceByKey((x,y)=>x+y)
        println("The jdbc_original_sum_num_rdd length is: "+jdbc_original_sum_num_rdd.count())

        /**
         * Calculating the max version of each crash_key
         */
        val jdbc_original_max_version_rdd = jdbc_original_rdd.map(e=>(e._2,e._4)).reduceByKey((x,y)=>if(x>y) x else y)
        println("The jdbc_original_max_version_rdd length is: "+jdbc_original_max_version_rdd.count())

        /**
         * Merge the original data
         */
        val jdbc_first_merge_rdd = (jdbc_original_sum_num_rdd.join(jdbc_original_max_version_rdd)).map(e=>(e._1,(e._2._1,e
          ._2._2)))
        println("The jdbc_first_merge_rdd length is: "+jdbc_first_merge_rdd.count())
        val jdbc_original_merge_rdd = (jdbc_original_rdd.map(e=>(e._2,e._1)).distinct()).join(jdbc_first_merge_rdd)

        val jdbc_original_finally_rdd = jdbc_original_merge_rdd.map(e=>(e._2._1,e._1,e._2._2._1,e._2._2._2))
        /**
         * Getting the info from the summary table
         */
        println("==================================")
        println(minId(util,inputDay,true))
        println(maxId(util,inputDay,true))
        println("==================================")
        val jdbc_summary_rdd = new JdbcRDD(sc,
          () =>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
          },
          "SELECT crash_key, crash_key_md5,crash_num,create_day,reappear_day,resolve_day," +
            "resolve_version,is_resolve,reappear from medusa_crash_summary_info where id >= ? and id<=?",
          minId(util,inputDay,true),
          maxId(util,inputDay,true),
          numOfPartition,
          r=>(r.getString(1),r.getString(2),r.getLong(3),r.getString(4),r.getString(5),r.getString(6),r.getString(7),r
            .getLong(8),r.getLong(9))
        )

        println("==================Having get the original info=====================")
        println("The length of original is: "+jdbc_original_finally_rdd.count())
        println("==================Having get the summary info=========================")
        println("The length of meta is: "+jdbc_summary_rdd.count)

        val tempOrdinaryRdd = jdbc_original_finally_rdd.map(e=>(e._1,e._2,e._3,e._4)).persist(StorageLevel.DISK_ONLY)
        println("tempOrdinaryRdd num is: "+tempOrdinaryRdd.count())


        /**
         * @1:the new crash has not appear in the meta_info table
         */
        println("================Begin insert new crash into meta_info table=================")
        val key_md5_list = jdbc_summary_rdd.map(e=>e._2).collect()
        println("Total version_key_trace_list is: "+key_md5_list.length)
        val new_appear_info = tempOrdinaryRdd.filter(e => !key_md5_list.contains(e._2)).collect()
        println("New crash num is:" + new_appear_info.length)
        val insertNew = "INSERT INTO medusa_crash_summary_info(crash_key,crash_key_md5,crash_num,create_day) VALUES(?,?,?,?)"
        new_appear_info.foreach(e=>{
          util.insert(insertNew,e._1, e._2,new JLong(e._3),inputDay)
        })

        /**
         * Deal with the case: crash has not been resolved!
         * @2: the new crash has appear in the meta_info table
         */
        println("=============Begin merge old data and new data=========================")
        val un_resolved_rdd = jdbc_summary_rdd.filter(_._8==0).filter(_._4<inputDay).cache()
        println("Un_resolved_rdd num is: "+un_resolved_rdd.count)
        val merge_info = un_resolved_rdd.map(e=>((e._2,e._1),(e._1,e._3,e._4,e._5,e._6,e._7,e._8,e._9))).
          join(tempOrdinaryRdd.map(e=>((e._2,e._1),(e._3,0,0,0,0,0,0,0))))
        println("The merged num is: "+merge_info.count)
        val new_merge_info = merge_info.map(e=>(e._1._1,(e._2._1._1,e._2._1._2+e._2._2._1,e._2._1._3,e._2._1._4,e._2._1._5,
          e._2._1._6,e._2._1._7,e._2._1._8))).collect()
        println("The crash has not resolved is: "+new_merge_info.length)
        new_merge_info.foreach(e=>{
          val updateSql = s"UPDATE medusa_crash_summary_info SET crash_num = ? WHERE crash_key_md5 = '${e._1}'"
          util.update(updateSql,new JLong(e._2._2))
        })

        /**
         * Deal with the case: the crash have been resolved
         *@3:the new crash is the reappear
         * Need considering the app_version_name is higher and the crash_num > 20
         */
        println("=========================Begin statistic reappear data===========================")

        val resolved_rdd = jdbc_summary_rdd.filter(_._8==1).cache()
        if(resolved_rdd.collect().length>0){
          val reappear_merge_info = resolved_rdd.map(e=>(e._2,(e._1,e._3,e._4,e._5,e._6,e._7,e._8,e._9))) join tempOrdinaryRdd
            .map(e=>(e._2,(e._3,e._4)))
          val reappear_new_merge_info = reappear_merge_info.map(e=>
            {(e._2._1._2+e._2._2._1)>=20 && e._2._2._2 >= e._2._1._6}
          match {
            case true=>(e._2._1._1,e._1,e._2._1._2+e._2._2._1,e._2._1._3,inputDay,"","",0l,e._2._1
              ._8+1)
            case false=>(e._2._1._1,e._1,e._2._1._2+e._2._2._1,e._2._1._3,e._2._1._4,e._2._1._5,e._2._1._6,e._2._1._7,e._2._1
              ._8)
          }).collect()

          reappear_new_merge_info.foreach(e=>{
            val updateSql = s"UPDATE medusa_crash_summary_info SET crash_num = ?,reappear_day = ?,resolve_day = ?," +
              s"resolve_version = ?, is_resolve = ?,reappear= ? WHERE crash_key_md5 = '${e._2}'"
            util.update(updateSql,new JLong(e._3),e._5,e._6,e._7,new JLong(e._8),new JLong(e._9))
          })
        }

        tempOrdinaryRdd.unpersist()
      }
      case None => {throw new RuntimeException("Needs one param!")}
    }
  }
}
