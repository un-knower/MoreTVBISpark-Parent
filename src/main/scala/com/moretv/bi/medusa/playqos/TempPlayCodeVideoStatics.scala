package com.moretv.bi.medusa.playqos

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.DataBases
import com.moretv.bi.util.ParamsParseUtil
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by witnes on 9/7/16.
  */

object TempPlayCodeVideoStatics extends BaseClass {

  private val videoSidList1 = List("g65gfg6le5d4","tvn8wybcs9vw","tvn8wyx0l79v","g65gl7l7hjvw","tvn8wybc7pvw","g65gkmo84gce","g65g45bc3f5h","tvn8wy5he52d","tvn8wyoqjkbc","g6f5m7b23fl7","g6f5jkbcg6ij","tvn8wybctvu9","tvn8wy5hfg9v","g65g45uv23tu","tvn8wyvwwx12","tvn8wyvwmo12","g65g34u9hju9","tvn8wyvw7nx0","g65gfgfh5i4f","tvn8wybc9vbc","g6f5nomnfhvx","tvn8wy12gi2d","g65gef5gg6m7","g65gh65g5ia2","g65gmntufhde","g65geftu5iwx","g65g4512e5uv","tvn8wyvw7o9v","g65g8qp85ix0","g65gc33fhj9v","g65gl745fh12","tvn8wyvwp89v","g65g2drthjd3","g6f5m7i6hj9x","tvn8wy34hibc","g65gjl3ee5ce","tvn8wyabnobc","tvn8wyfgnp12","tvn8wyabe4vw","tvn8wyab2cab","g65gl7km3fnp","g6f54fqs4gwx","g6f5noij238q","tvn8wy5hsuwx","g65gef7og63f","tvn8wy34tv2d","tvn8wyvwk7u9","tvn8ik123fx0","g6f5f5prd4wx","tvn86labmnx0")
 private val videoSidList2 = List("g6i62dfgcea2","tvn8xzd3novw","tvn8xzd3qr9v","g6i6gif5d49x","g6i645acd4cd","g6i6ij8r3fwx","g6i6jlmnhjac","g6i6mn6khj7p","tvn8xz1bk7x0","g6i62duwcebc","tvn8xzacc3ab","g6i66kqr3f9x","tvn8xzwyxzu9","g6hi7ntve52c","g6hid3fhfhbd","g6i6x0tuhjst","g6hikljm4gjm","g6i6bcop4gxy","g6hijk6l4ge5","tvn8xza1m7ab","g6i6bcqtcekm","tvn8xza13ftu","g6i6rt4ffh9x","g6hid36l5it9","g6highqtd4t9","g6i612d323ce","tvn8xz1b7nbc","g6i634vwfhe4","g6i62de5e5k7","g6hikl3fe5ln","g6hi6jvxceno","g6i6ab1ce5p8","tvn8wykmn8vw","tvn8xza1x0u9","g6i68rbcfhhj","tvn8xz7p6l9v","g6hixzbdfhwx","g6high4f3fb2","tvn8xzb2uvwx","tvn8xz2ca2x0","g6i6rtik5iop","g6hiuw34e5qr","g6hii64f234f","tvn8xzb2qswx","g6hiuwef4gkl","tvn8xzb24gx0","tvn8xzs9qtu9","tvn8xzruqtwx","g6i6rtopfhbd","g6i68rruhj7p")

  def main(args: Array[String]): Unit = {
    ModuleClass.executor(this,args)
  }

  override def execute(args: Array[String]): Unit = {

    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

        val inputPath1 = s"/log/medusa/parquet/20170401/playqos"
        val inputPath2 = s"/log/medusa/parquet/20170408/playqos"

        if(p.deleteOld){
          util.delete(s"delete from temp_play_code_video_statistics")
        }

        val rdd1 = sqlContext.read.parquet(inputPath1).select("jsonLog")
        val sql = "insert into temp_play_code_video_statistics(video_sid,day,play_code,num) values(?,?,?,?)"

        //(videoSid, day, playcode)
        val result1 = rdd1.map(e=>getPlayCode(1,"2017-03-31",e.getString(0))).
        filter(_!=null).flatMap(x=>x)
          .countByValue()

        result1.foreach(x => {
          util.insert(sql,x._1._1,x._1._2,x._1._3,x._2)
          println(sql,x._1._1,x._1._2,x._1._3,x._2)
        })

        val rdd2 = sqlContext.read.parquet(inputPath2).select("jsonLog")

        //(videoSid, day, playcode)
        val result2 = rdd2.map(e=>getPlayCode(2,"2017-04-07",e.getString(0))).
          filter(_!=null).flatMap(x=>x)
          .countByValue()

        result2.foreach(x => {
          util.insert(sql,x._1._1,x._1._2,x._1._3,x._2)
          println(sql,x._1._1,x._1._2,x._1._3,x._2)
        })
        util.destory()

      }
      case None => {
        throw new RuntimeException("At least need param --startDate.")
      }
    }


  }


  /**
    *
    * @param day
    * @param str json字符串
    * @return (videoSid, day, playcode)
    */
  def getPlayCode(flag:Int, day: String, str: String) = {

    val res = new ListBuffer[(String, String, Int)]()

    try {
      val jsObj = new JSONObject(str)

      val videoSid = jsObj.optString("videoSid")
      val isContained = if(flag == 1){
        videoSidList1.contains(videoSid)
      }else{
        videoSidList2.contains(videoSid)
      }
      if(isContained){
        val playqosArr = jsObj.optJSONArray("playqos")

        if (playqosArr != null) {

          (0 until playqosArr.length).foreach(i => {
            val playqos = playqosArr.optJSONObject(i)
            val sourcecases = playqos.optJSONArray("sourcecases")

            if (sourcecases != null) {
              (0 until sourcecases.length).foreach(w => {
                val sourcecase = sourcecases.optJSONObject(w)
                res.+=((videoSid, day,sourcecase.optInt("playCode")))
              })
            }
          })
        }
      }
      res.toList

    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        System.err.println(str)
        null
      }
    }

  }

}
