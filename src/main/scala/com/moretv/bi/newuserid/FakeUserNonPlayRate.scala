package com.moretv.bi.newuserid

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.sdk.dataOps.MySqlOps
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}

/**
  * Created by 连凯 on 2017/8/23.
  * 假用户无用户行为占比
  * tablename: medusa.fake_user_non_play_rate
  *
  */
object FakeUserNonPlayRate extends BaseClass {
  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val s = sqlContext
        import s.implicits._

        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val dayCN = DateFormatUtils.cnFormat.format(cal.getTime)
          val logDates = getNextTwoWeekDates(DateFormatUtils.readFormat.format(cal.getTime))

          if(p.deleteOld){
            val deleteSql = "delete from fake_user_non_play_rate where day = ?"
            util.delete(deleteSql,dayCN)

          }
          val sqlMinMaxId = "select min(id),max(id) from mtv_account_migration_vice"
          val sqlData = s"select user_id from mtv_account_migration_vice where id >= ? and id <= ? and left(openTime,10) = '$dayCN'"

          MySqlOps.getJdbcRDD(sc,DataBases.MORETV_MEDUSA_MYSQL,sqlMinMaxId,sqlData,100,rs => rs.getString(1)).
            toDF("userId").registerTempTable("new_user_data")

          sqlContext.cacheTable("new_user_data")

          val newUserNum = sqlContext.sql("select count(userId) from new_user_data").collect().head.getLong(0)

          sqlContext.read.load(s"/log/medusa/parquet/$logDates/{play,detail,homeview}").
            select("userId").distinct().registerTempTable("log_data")

          val actionUser = sqlContext.sql("select count(a.userId) from new_user_data a join log_data b on a.userId = b.userId").
            collect().head.getLong(0)

          val insertSql = "insert into fake_user_non_play_rate(day,new_user,action_user) values(?,?,?)"
          util.insert(insertSql,dayCN,newUserNum,actionUser)
          sqlContext.uncacheTable("new_user_data")

          cal.add(Calendar.DAY_OF_MONTH,1)
        })

        util.destory()
      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }
    }

  }


  def getNextTwoWeekDates(enDateStr:String):String = {
    val enFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(enFormat.parse(enDateStr))
    cal.add(Calendar.DAY_OF_MONTH,-1)
    val now = enFormat.format(Calendar.getInstance().getTime)
    val dateStrs = (0 until 14).map(i => {
      cal.add(Calendar.DAY_OF_MONTH,1)
      val date = enFormat.format(cal.getTime)
      if(date.compareTo(now) > 0) null else date
    }).filter(_!=null).mkString(",")
    s"{$dateStrs}"
  }
}
