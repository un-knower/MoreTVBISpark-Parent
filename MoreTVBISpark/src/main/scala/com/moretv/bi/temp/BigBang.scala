package com.moretv.bi.temp

import com.moretv.bi.util.FileUtils
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 10/13/16.
  */
object BigBang extends BaseClass {

  def main(args: Array[String]) {
    ModuleClass.executor(BigBang, args)
  }

  override def execute(args: Array[String]): Unit = {

    val dataUserPath = "/log/medusa/temple/gradationUsers"

    val users = sqlContext.read.parquet(dataUserPath).map(e => "'" + e.getString(0) + "'").collect.mkString(",")

    val loadPath = "/log/medusa/parquet/20161014/{enter,play}"

    val load2Path = "/log/medusa/parquet/20161014/{play}"

 // sqlContext.read.parquet(loadPath).filter("event in ('startplay','enter')").registerTempTable("log_data")

    sqlContext.read.parquet(load2Path).filter("event in ('userexit','selfend')").registerTempTable("log_data1")

//    val df1 = sqlContext.sql(s"select date, event, count(userId) as pv, count(distinct userId) as uv from log_data " +
    //      s"where userId in ($users) and date = '2016-10-13' group by date, event ")
    //
    //    df1.show(50,false)

  //  df1.rdd.saveAsTextFile("/tmp/text1")
    try{
      val df2 = sqlContext.sql("select date, sum(duration) from log_data1 " +
        s"where userId in ($users) and duration < 10800 and duration > 0 and date = '2016-10-13' group by date ")

      df2.show(10, false)
    }catch {
      case e:Exception => println(e)
    }


  //  df2.rdd.saveAsTextFile("/tmp/text2")



  }

}
