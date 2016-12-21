package src.com.moretv.bi.report.medusa.CrashLog

/**
 * Created by Administrator on 2016/3/30.
 */

import java.lang.{Long=>JLong}
import com.moretv.bi.util._
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


object CrashMetaInfo extends BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        val input = p.startDate
        val inputDay = DateFormatUtils.toDateCN(input)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val url = util.prop.getProperty("url")
        val driver = util.prop.getProperty("driver")
        val user = util.prop.getProperty("user")
        val password = util.prop.getProperty("password")

        /**
         * Define two lambda functions
         */
        val minId = (util: MySqlOps,inputDay:String,flag:Boolean) => {
          flag match {
            case true=>{
              val sql = "SELECT Min(id) from medusa_crash_meta_secondary_phase_info"
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
              val sql = "SELECT MAX(id) FROM medusa_crash_meta_secondary_phase_info"
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

      /*  val jdbc_original_rdd_pri = new JdbcRDD(sc,
          () => {
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true", "bi", "mlw321@moretv")
          },
          "SELECT day, app_version_name, android_version, date_code, product_code,stack_trace,stack_trace_md5," +
            "crash_num from medusa_crash_original_secondary_phase_info where id >=? and id <= ? ",
          minId(util,inputDay,false),
          maxId(util,inputDay,false),
          numOfPartition,
          r => (r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), r.getString(6), r.getString
            (7), r.getLong(8))
        )
*/

        val jdbc_original_rdd_sql = "SELECT day, app_version_name, android_version, date_code, product_code,stack_trace,stack_trace_md5," +
          "crash_num from medusa_crash_original_secondary_phase_info where id >=? and id <= ? "
        val jdbc_original_rdd_pri =MySqlOps.getJdbcRDD(sc,jdbc_original_rdd_sql,"medusa_crash_original_secondary_phase_info",r=>(r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), r.getString(6), r.getString
        (7), r.getLong(8)),driver,url,user,password,(minId(util,inputDay,false),maxId(util,inputDay,false)),numOfPartition)

        val jdbc_original_rdd = jdbc_original_rdd_pri.filter(_._2.length<=30).filter(_._3.length<=30)
        /**
         * Getting the info from the meta table
         */
       /* val jdbc_meta_rdd = new JdbcRDD(sc,
          () =>{
            Class.forName("com.mysql.jdbc.Driver")
            DriverManager.getConnection("jdbc:mysql://10.10.2" +
              ".15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
          },
          "SELECT create_day, update_day, app_version_name, android_version, date_code, product_code, " +
            "stack_trace, stack_trace_md5, crash_num FROM medusa_crash_meta_secondary_phase_info where id >= ? and id<=?",
          minId(util,inputDay,true),
          maxId(util,inputDay,true),
          numOfPartition,
          r=>(r.getString(1),r.getString(2),r.getString(3),r.getString(4),r.getString(5),r.getString(6),r.getString(7),r
            .getString(8),r.getLong(9))
        )*/

        val jdbc_meta_rdd_sql = "SELECT create_day, update_day, app_version_name, android_version, date_code, product_code, " +
          "stack_trace, stack_trace_md5, crash_num FROM medusa_crash_meta_secondary_phase_info where id >= ? and id<=?"
        val jdbc_meta_rdd =MySqlOps.getJdbcRDD(sc,jdbc_meta_rdd_sql,"medusa_crash_meta_secondary_phase_info",r=>(r.getString(1),r.getString(2),r.getString(3),r.getString(4),r.getString(5),r.getString(6),r.getString(7),r
          .getString(8),r.getLong(9)),driver,url,user,password,(minId(util,inputDay,true),maxId(util,inputDay,true)),numOfPartition)


        val insertNew = "INSERT INTO medusa_crash_meta_secondary_phase_info(create_day,update_day,app_version_name," +
          "android_version,date_code,product_code,stack_trace,stack_trace_md5,crash_num) VALUES(?,?,?,?,?,?,?,?,?)"

        /**
         * @1:the new crash has not appear in the meta_info table
         */
        println("================Begin insert new crash into meta_info table=================")
        val version_key_trace_list = jdbc_meta_rdd.map(e=>(e._3,e._4,e._5,e._6,e._8)).collect()
        val new_appear_info = jdbc_original_rdd.filter(e => !version_key_trace_list.contains((e._2,e._3,e._4,e._5,e._7)))
          .collect()
        new_appear_info.foreach(e=>{
          try{
            util.insert(insertNew,e._1,e._1, e._2,e._3,e._4,e._5,e._6,e._7,new JLong(e._8))
          }catch {
            case e:Exception => e.printStackTrace()
          }

        })


          /**
           * Deal with the case: crash has not been resolved!
           * @2: the new crash has appear in the meta_info table
           */
        println("=============Begin merge old data and new data=========================")
          val un_resolved_rdd = jdbc_original_rdd.filter(e =>version_key_trace_list.contains((e._2,e._3,e._4,e._5,e._7)))
          val merge_info = jdbc_meta_rdd.map(e=>((e._3,e._4,e._5,e._6,e._8),e._9)) join un_resolved_rdd.map(e=>((e._2,e
            ._3,e._4,e._5,e._7),(e._1,e._8)))
          val new_merge_info = merge_info.map(e=>(e._2._2._1,e._1._1,e._1._2,e._1._3,e._1._4,e._1._5,e._2._1+e._2._2._2))
            .collect()
            new_merge_info.foreach(e=>{
            val updateSql = s"UPDATE medusa_crash_meta_secondary_phase_info SET update_day=?, crash_num = ? WHERE " +
              s"app_version_name='${e._2}' and android_version='${e._3}' and date_code='${e._4}' and product_code='${e._5}' and " +
              s"stack_trace_md5='${e._6}'"
            util.update(updateSql,e._1,new JLong(e._7))
          })
      }
      case None => {
        throw new RuntimeException("Needs the param: startDate!")
      }
    }
  }
}
