package com.moretv.bi.medusa.playqos

import java.io.{File, PrintWriter}
import java.util.Calendar

import cn.whaley.sdk.utils.DataFrameUtil
import com.moretv.bi.util.{DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
/**
  * Created by hh on 2017/9/7.
  */
object MuGuVParseDefeat extends  BaseClass{

  def main(args: Array[String]) {
    ModuleClass.executor(this,args)
  }

  override def execute(args :Array[String]) = {

    ParamsParseUtil.parse(args) match {
      case Some(p)=>{
        val startDate = p.startDate
        val cal = Calendar.getInstance
        cal.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val sql = "select subString(datetime,12,2),jsonLog from log_data "
          val df =  DataFrameUtil.getDFByDateWithSql("medusa",date,"playqos",sql)
            .map(e=>(e.getString(0),e.getString(1))).filter(_!=null)

         // DataFrameUtil.getDFByDateWithSql("medusa",date,"playqos",sql).show()

          //统计蘑菇源的播放量和解析失败量
          val playNum = df.map(e => {
            try {
              val sourceDetailInfo = ListBuffer[(String,String,String)]()
              val temp = new JSONObject(e._2)
              val playqosArr = temp.getJSONArray("playqos")
              for (i <- 0 until playqosArr.length()) {
                val obj = playqosArr.getJSONObject(i)
                val source = obj.getString("videoSource")
                val tryList = obj.getJSONArray("sourcecases")
                for(i <- 0 until tryList.length() ){
                  val tryObj = tryList.getJSONObject(i)
                  val playCode = tryObj.getString("playCode")
                  val result = (e._1,source,playCode)
                  sourceDetailInfo += result
                }
              }
              sourceDetailInfo
              } catch {
                case e: Exception => {
                  println("************Parse the log failed***************")
                  e.printStackTrace()
                  null
                }
              }
            }).filter(_ != null).flatMap(e => e.toList).filter(e=>e._2 == "moguv").map(e=>(e._1,e._3))

          playNum.take(10).foreach(r=>{
            println("***************Print the play info: " + r)
          })
          val totalNum = playNum.countByKey()
          val successNum = playNum.filter(_._2 == "200").countByKey()
          val parseNum = playNum.filter(_._2 == "-1").countByKey()
          val autoNum = playNum.filter(_._2 == "-2").countByKey()
          val playDefeatNum = playNum.filter(_._2 == "-3").countByKey()

          val file = new File(s"../file/$date+muguv.csv")
          val out = new PrintWriter(file)

          val keys = totalNum.keySet

          keys.foreach(e =>{
            out.println(e+"\t" + totalNum(e)+ "\t" + successNum(e) + "\t" + parseNum(e)+ "\t" + autoNum(e)+"\t" + playDefeatNum(e))
          })

         /* totalNum.foreach(e =>{
            out.println(e._1+"\t" + totalNum(e._1))
          })*/

          out.close()
          cal.add(Calendar.DAY_OF_MONTH, -1)


        })



      }
      case  None =>{
        throw  new RuntimeException("At least need parm --startDate")
      }
    }

  }

}
