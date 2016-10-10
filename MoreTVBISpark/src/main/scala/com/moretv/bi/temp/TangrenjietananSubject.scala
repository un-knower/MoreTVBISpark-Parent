package com.moretv.bi.temp

import java.io.{File, PrintWriter}
import com.moretv.bi.common.SubjectPlayviewPVUV

import java.lang.{Long => JLong}

import com.moretv.bi.util.{SparkSetting, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object TangrenjietananSubject extends SparkSetting{
  def main (args: Array[String]) {
    config.setAppName("TangrenjietananSubject")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)

    ParamsParseUtil.parse(args) match {
      case Some(p) => {
        //implicit val sQLContext = SQLContext.getOrCreate(sc)

        val file = new File("/home/moretv/mbi/zhehua/file/temp/subjectrecommend.csv")
        val out = new PrintWriter(file)
        val subject = "movie764"
        val sid1 = "s9n8mndecdtu"
        val sid2 = "s9n8l7np5gwx"
        val sid3 = "s9n8l7np3e2d"
        val sid4 = "s9n8l7np1bu9"
        val sid5 = "s9n8l7npa12d"
        val sid6 = "s9n8l7npwy9v"
        val resArray1 = get2dayspvuv(subject,sid1,sqlContext)
        val resArray2 = get2dayspvuv(subject,sid2,sqlContext)
        val resArray3 = get2dayspvuv(subject,sid3,sqlContext)
        val resArray4 = get2dayspvuv(subject,sid4,sqlContext)
        val resArray5 = get2dayspvuv(subject,sid5,sqlContext)
        val resArray6 = get2dayspvuv(subject,sid6,sqlContext)
        out.println(sid1)
  /*      out2file(resArray1(0),resArray1(1),resArray1(2),out)
        out.println
        out.println(sid2)
        out2file(resArray2(0),resArray2(1),resArray2(2),out)
        out.println
        out.println(sid3)
        out2file(resArray3(0),resArray3(1),resArray3(2),out)
        out.println
        out.println(sid4)
        out2file(resArray4(0),resArray4(1),resArray4(2),out)
        out.println
        out.println(sid5)
        out2file(resArray5(0),resArray5(1),resArray5(2),out)
        out.println
        out.println(sid6)
        out2file(resArray6(0),resArray6(1),resArray6(2),out)
*/
        out.close()
      }
      case None => {
        throw new RuntimeException("At least need param --excuteDate.")
      }
    }
  }
  def get2dayspvuv(subject:String, sid:String,sqlContext: SQLContext) = {
    val inputpath1 = "/mbi/parquet/playview/20151230/*"
    val inputpath2 = "/mbi/parquet/playview/20151231/*"
    val inputpath3 = "/mbi/parquet/playview/201512{30,31}/*"
    val result29 = SubjectPlayviewPVUV.getPVUV(subject,sid,inputpath1,sqlContext)
    val result30 = SubjectPlayviewPVUV.getPVUV(subject,sid,inputpath2,sqlContext)
    val result = SubjectPlayviewPVUV.getPVUV(subject,sid,inputpath3,sqlContext)
    Array(result29,result30,result)
  }
  def out2file(res29:Array[Long], res30:Array[Long], res:Array[Long], out:PrintWriter): Unit ={
    out.print("2015-12-29    ")
    res29.foreach(e => {
      out.print(e+"    ")
    })
    out.println

    out.print("2015-12-30    ")
    res30.foreach(e => {
      out.print(e+"    ")
    })
    out.println

    out.print("总计    ")
    res.foreach(e => {
      out.print(e+"    ")
    })
  }

}
