package com.moretv.bi.report.medusa.pageStatistics

import java.lang.{Long => JLong}
import java.util.Calendar

//import cn.whaley.jobsdk.{JobStatus, SendMail}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}

/**
  * Created by witnes on 9/13/16.
  */
object StartPageStatistics extends  BaseClass{

  private val tableName = "medusa_startpage_view_info"



  def main(args: Array[String]): Unit = {
 //   JobStatus.getConfig(appName)
    ModuleClass.executor(this,args)

  }
  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        //init util
        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        //params
        val startDate =  p.startDate
        //date
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{

          val loadDate = DateFormatUtils.readFormat.format(cal.getTime)
          cal.add(Calendar.DAY_OF_MONTH,-1)
          val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

          val df=DataIO.getDataFrameOps.getDF(sqlContext,p.paramMap,MEDUSA,LogTypes.MEDUSABOOTSTARTPAGE)

          val startPageDf = df.select("apkVersion","pageType","userId")
                .filter("pageType is not null")

          val startPageRdd =startPageDf.map(e=>((e.getString(0),e.getString(1)),e.getString(2)))

          //aggregate
          val pvRdd = startPageRdd.countByKey
          val uvRdd = startPageRdd.distinct.countByKey

          if(p.deleteOld){
            util.delete(s"delete from $tableName where day = ?", sqlDate)
          }

          // get values & deal with table
          pvRdd.foreach( e=>{
            val key = e._1
            val pv = e._2

            val uv = uvRdd.get(e._1) match {
              case Some(p) => p
              case None => 0
            }

            println(key._1,key._2, new JLong(pv), new JLong(uv))

            // insert
            try{
              util.insert(s"insert into $tableName(day,apkversion,pagetype,view_num,user_num)values(?,?,?,?,?)",
                sqlDate,key._1,key._2,new JLong(pv), new JLong(uv))
            }catch {
              case e => {println(e)}
            }


            })

        })

      }
      case None => {
        throw new RuntimeException("At least needs one param: startDate!")
      }

    }

  }
}
