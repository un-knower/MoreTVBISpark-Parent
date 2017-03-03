package com.moretv.bi.report.medusa.medusaGrayTestingStatistic

import java.util.Calendar
import java.lang.{Long => JLong}

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{LogTypes, DataBases}
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import org.apache.spark.storage.StorageLevel

/**
 * Created by Administrator on 2016/5/11.
 */
object CrashAndLoginInfoByVersionAndProduct extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //val sc = new SparkContext(config)
        //val sqlContext = new SQLContext(sc)
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val url = util.prop.getProperty("url")
        val driver = util.prop.getProperty("driver")
        val user = util.prop.getProperty("user")
        val password = util.prop.getProperty("password")

       /* val logType = "homeview"
        val fileDir = "/log/medusa/parquet/"*/
        val sqlSpark = "select apkVersion,productModel,count(userId),count(distinct userId) from log_data " +
          "where event='enter' group by apkVersion,productModel"
        val sqlInsert = "insert into medusa_crash_login_info_by_product_version(day,apk_version,productModel," +
          "login_num,login_user,crash_num,crash_user) values (?,?,?,?,?,?,?)"
        val sqlDelete = "delete from medusa_crash_login_info_by_product_version where day = ?"
        val startDate = p.startDate

        val calDir = Calendar.getInstance()
        calDir.setTime(DateFormatUtils.readFormat.parse(startDate))

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))
        cal.add(Calendar.DAY_OF_MONTH,-1)

        (0 until p.numOfDays).foreach(i=>{
          val dayDir = DateFormatUtils.readFormat.format(calDir.getTime)
          val day = DateFormatUtils.readFormat.format(cal.getTime)
          val date = DateFormatUtils.toDateCN(day)
          // 获取每天login的人数与次数
          // 从parquet中获取数据
          //val logData = sqlContext.read.parquet(s"$fileDir$dayDir/$logType").persist(StorageLevel.DISK_ONLY)
          val df = DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.HOMEVIEW,dayDir).persist(StorageLevel.DISK_ONLY)

          df.select("apkVersion","productModel","userId","event").registerTempTable("log_data")
          val loginInfoDf = df.sqlContext.sql(sqlSpark)
          val loginInfoRdd = loginInfoDf.map(e=>(e.getString(0),e.getString(1),e.getLong(2),e.getLong(3)))
          // 获取每天crash的人数与次数
          val minIdNum = util.selectOne(s"select min(id) from medusa_crash_product_version_num where day='$date'")(0)
            .toString.toLong
          val maxIdNum = util.selectOne(s"select max(id) from medusa_crash_product_version_num where day='$date'")(0)
            .toString.toLong
          val minIdUser = util.selectOne(s"select min(id) from medusa_crash_product_version_user where day='$date'")(0)
            .toString.toLong
          val maxIdUser = util.selectOne(s"select max(id) from medusa_crash_product_version_user where day='$date'")(0)
            .toString.toLong
          val numOfPartition = 20
        /*  val crashNumRdd = new JdbcRDD(sc,
            ()=>{
              Class.forName("com.mysql.jdbc.Driver")
              DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
            },
            "select day,app_version_name,product_code,total_number from medusa_crash_product_version_num where id >= ? and" +
              " id <= ?",
            minIdNum,
            maxIdNum,
            numOfPartition,
            r=>(r.getString(1),r.getString(2),r.getString(3),r.getLong(4))
          )*/
          val sqlInfo ="select day,app_version_name,product_code,total_number from medusa_crash_product_version_num where id >= ? and" +
            " id <= ?"
          val crashNumRdd = MySqlOps.
            getJdbcRDD(sc,sqlInfo,"medusa_crash_product_version_num",r=>(r.getString(1),r.getString(2),r.getString(3),r.getLong(4)),driver,url,user,password,(minIdNum ,maxIdNum ),numOfPartition)

         /* val crashUserNum = new JdbcRDD(sc,
            ()=>{
              Class.forName("com.mysql.jdbc.Driver")
              DriverManager.getConnection("jdbc:mysql://10.10.2.15:3306/medusa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true","bi","mlw321@moretv")
            },
            "select day,app_version_name,product_code,total_user from medusa_crash_product_version_user where id >= ? and" +
              " id <= ?",
            minIdUser,
            maxIdUser,
            numOfPartition,
            r=>(r.getString(1),r.getString(2),r.getString(3),r.getLong(4))
          )*/
          val  crashUserNumSql="select day,app_version_name,product_code,total_user from medusa_crash_product_version_user where id >= ? and" +
            " id <= ?"
          val crashUserNum = MySqlOps.
            getJdbcRDD(sc,crashUserNumSql,"medusa_crash_product_version_user",r=>(r.getString(1),r.getString(2),r.getString(3),r.getLong(4)),driver,url,user,password,(minIdUser,maxIdUser),numOfPartition)


          // 合并crashNumRdd与crashUserRdd的数据
          val crashMerger = crashNumRdd.map(e=>((e._1,e._2,e._3),e._4)) join (crashUserNum.map(e=>((e._1,e._2,e._3),e._4)))
          val crashInfoRdd = crashMerger.map(e=>(e._1._2,e._1._3,e._2._1,e._2._2))
          // 合并crash与login信息
          val crashAndLoginRdd = loginInfoRdd.map(e=>((e._1,e._2),(e._3,e._4))) join (crashInfoRdd.map(e=>((e._1,e._2),(e
            ._3,e._4))))
          // 依次为版本号、产品号、登录次数、登录人数、crash次数、crash人数
          val resultRdd = crashAndLoginRdd.map(e=>(e._1._1,e._1._2,e._2._1._1,e._2._1._2,e._2._2._1,e._2._2._2))
          if(p.deleteOld){
            util.delete(sqlDelete,date)
          }

          resultRdd.collect.foreach(e=>{
            util.insert(sqlInsert,date,e._1,e._2,new JLong(e._3),new JLong(e._4),new JLong(e._5),new JLong(e._6))
          })

          val sumInfoByVersion = resultRdd.map(e=>(e._1,(e._3,e._4,e._5,e._6))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x
            ._3+y._3,x._4+y._4))
          sumInfoByVersion.collect().foreach(i=>{
            util.insert(sqlInsert,date,i._1,"All",new JLong(i._2._1),new JLong(i._2._2),new JLong(i._2._3),new JLong(i._2
              ._4))
          })
          cal.add(Calendar.DAY_OF_MONTH,-1)
          calDir.add(Calendar.DAY_OF_MONTH,-1)
          //logData.unpersist()
          df.unpersist()
        })
      }
      case None => {}
    }
  }
}
