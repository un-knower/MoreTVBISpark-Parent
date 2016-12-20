package com.moretv.bi.report.medusa.CrashLog

/**
 * Created by Administrator on 2016/4/6.
 */

import java.lang.{Long=>JLong}
import com.moretv.bi.report.medusa.util.FilterUnicodeString
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import java.sql.DriverManager
import org.apache.spark.storage.StorageLevel

object CrashSummaryInfo extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(CrashSummaryInfo,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val input = p.startDate
        val inputDay = DateFormatUtils.toDateCN(input)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        /**
         * Define two lambda functions
         */
        val minId = (util: MySqlOps,inputDay:String,flag:Boolean) => {
          flag match {
            case true=>{
              val sql = "SELECT Min(id) from medusa_crash_summary_secondary_phase_info"
              val arr = util.selectOne(sql)
              arr(0).toString.toLong
            }
            case false=>{
              val sql = s"SELECT Min(id) from medusa_crash_original_secondary_phase_info WHERE day= '$inputDay'"
              val arr = util.selectOne(sql)
              arr(0).toString.toLong
            }
          }
        }
        val maxId = (util: MySqlOps,inputDay:String,flag:Boolean) => {
          flag match {
            case true=>{
              val sql = "SELECT MAX(id) FROM medusa_crash_summary_secondary_phase_info"
              val arr = util.selectOne(sql)
              arr(0).toString.toLong
            }
            case false=>{
              val sql = s"SELECT MAX(id) from medusa_crash_original_secondary_phase_info where day='$inputDay'"
              val arr = util.selectOne(sql)
              arr(0).toString.toLong
            }
          }
        }

        val numOfPartition = 20

        /**
         * Getting the data from original table, which includes the information of each day
         */

        val jdbcOriginalRdd = new JdbcRDD(sc,
          () => {
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
          },
          "SELECT stack_trace_md5,stack_trace,app_version_name, crash_num " +
            "from medusa_crash_original_secondary_phase_info where id >=? and id <= ? ",
          minId(util,inputDay,false),
          maxId(util,inputDay,false),
          numOfPartition,
          r => (r.getString(1), r.getString(2), r.getString(3),r.getLong(4))
        )
        /**
         * Calculating the sum_crash_num of each crash_key
         * 需要过滤乱码的问题
         */
//        val jdbc_original_rdd = jdbc_original_from_db_rdd.filter(e=>FilterUnicodeString.filterUnicodeString(e._1))
//        val jdbc_original_rdd = jdbcOriginalRdd

        /*计算每个stack_trace的总次数*/
        val jdbcOriginalSumNumRdd = jdbcOriginalRdd.map(e=>(e._1,e._4)).reduceByKey((x,y)=>x+y)
        /*计算每个stack_trace的最大版本*/
        val jdbcOriginalMaxVersionRdd = jdbcOriginalRdd.map(e=>(e._1,e._3)).reduceByKey((x,y)=>if(x>y) x else y)

        /**
         * Merge the original data
         */
        val jdbcFirstMergeRdd = (jdbcOriginalSumNumRdd.join(jdbcOriginalMaxVersionRdd)).map(e=>(e._1,(e._2._1,e
          ._2._2)))
        val jdbcOriginalMergeRdd = (jdbcOriginalRdd.map(e=>(e._1,e._2)).distinct()).join(jdbcFirstMergeRdd)
        /*stack_trace_md5\stack_trace\crash_num\crash_version*/
        val jdbcOriginalFinallyRdd = jdbcOriginalMergeRdd.map(e=>(e._1,e._2._1,e._2._2._1,e._2._2._2))
        val tempOrdinaryRdd = jdbcOriginalFinallyRdd.map(e=>(e._1,e._2,e._3,e._4)).persist(StorageLevel.DISK_ONLY)

        /**
         * Getting the info from the summary table
         */
        val jdbcSummaryRdd = new JdbcRDD(sc,
          () =>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
          },
          "SELECT stack_trace_md5, stack_trace,crash_num,create_day,reappear_day,resolve_day," +
            "resolve_version,is_resolve,reappear from medusa_crash_summary_secondary_phase_info where id >= ? and id<=?",
          minId(util,inputDay,true),
          maxId(util,inputDay,true),
          numOfPartition,
          r=>(r.getString(1),r.getString(2),r.getLong(3),r.getString(4),r.getString(5),r.getString(6),r.getString(7),r
            .getLong(8),r.getLong(9))
        )


        /**
         * @1:the new crash has not appear in the meta_info table
         */
        println("================Begin insert new crash into meta_info table=================")
        val stackTraceMD5List = jdbcSummaryRdd.map(e=>e._1).collect()
        val newAppearInfo = tempOrdinaryRdd.filter(e => !stackTraceMD5List.contains(e._1)).collect()
        val insertNew = "INSERT INTO medusa_crash_summary_secondary_phase_info(stack_trace_md5,stack_trace,crash_num," +
          "create_day) VALUES (?,?,?,?)"
        newAppearInfo.foreach(e=>{
          try{
            util.insert(insertNew,e._1, e._2,new JLong(e._3),inputDay)
          }catch {
            case e:Exception => e.printStackTrace()
          }

        })

        /**
         * Deal with the case: crash has not been resolved!
         * @2: the new crash has appear in the meta_info table
         */
        println("=============Begin merge old data and new data=========================")
        val unResolvedRdd = jdbcSummaryRdd.filter(_._8==0).filter(_._4<inputDay).cache()
        val mergeInfoTemp = unResolvedRdd.map(e=>((e._1,e._2),(e._3,e._4,e._5,e._6,e._7,e._8,e._9))).
          join(tempOrdinaryRdd.map(e=>((e._1,e._2),(e._3,0,0,0,0,0,0,0))))
        val mergeInfo=mergeInfoTemp.map(e=>(e._1._1,e._1._2,e._2._1._1+e._2._2._1,e._2._1._2,e._2._1._3,e._2._1._4,e._2._1
          ._5,e._2._1._6,e._2._1._7)).collect()
        mergeInfo.foreach(e=>{
          val updateSql = s"UPDATE medusa_crash_summary_secondary_phase_info SET crash_num = ? WHERE stack_trace_md5 = '${e
            ._1}'"
          util.update(updateSql,new JLong(e._3))
        })

        /**
         * Deal with the case: the crash have been resolved
         *@3:the new crash is the reappear
         * Need considering the app_version_name is higher and the crash_num > 20
         */
        println("=========================Begin statistic reappear data===========================")

        val resolvedRdd = jdbcSummaryRdd.filter(_._8==1).cache()
        if(resolvedRdd.collect().length>0){
          val reappearMergeInfo = resolvedRdd.map(e=>(e._1,(e._2,e._3,e._4,e._5,e._6,e._7,e._8,e._9))) join tempOrdinaryRdd
            .map(e=>(e._1,(e._2,e._3,e._4)))
          val reappearNewMergeInfo = reappearMergeInfo.map(e=>
            {(e._2._1._2+e._2._2._2)>=20 && e._2._2._3 >= e._2._1._6}
          match {
            case true=>(e._1,e._2._1._1,e._2._1._2+e._2._2._2,e._2._1._3,inputDay,"","",0l,e._2._1
              ._8+1)
            case false=>(e._1,e._2._1._1,e._2._1._2+e._2._2._2,e._2._1._3,e._2._1._4,e._2._1._5,e._2._1._6,e._2._1._7,e._2._1
              ._8)
          }).collect()

          reappearNewMergeInfo.foreach(e=>{
            val updateSql = s"UPDATE medusa_crash_summary_secondary_phase_info SET crash_num = ?,reappear_day = ?," +
              s"resolve_day = ?, resolve_version = ?, is_resolve = ?,reappear= ? WHERE stack_trace_md5 = '${e._1}'"
            util.update(updateSql,new JLong(e._3),e._5,e._6,e._7,new JLong(e._8),new JLong(e._9))
          })
        }

        tempOrdinaryRdd.unpersist()
      }
      case None => {throw new RuntimeException("Needs one param!")}
    }
  }
}
