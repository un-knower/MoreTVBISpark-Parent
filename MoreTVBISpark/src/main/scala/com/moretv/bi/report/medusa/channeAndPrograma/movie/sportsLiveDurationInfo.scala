package com.moretv.bi.report.medusa.channeAndPrograma.movie

import java.lang.{Long => JLong}
import java.util.Calendar

import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil, SparkSetting}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Created by Administrator on 2016/5/16.
 * 该统计少儿频道四级菜单的播放情况
 */
object sportsLiveDurationInfo extends BaseClass{
  private val regex=("(thirdparty|home)-kids_home-(kids_seecartoon|kids_songhome)-" +
    "(search|kids_star|p_shaoer_hot_1|kid_zhuanti|zongyi_shaoer|p_donghua1|p_donghua2|p_donghua3|ids-english" +
    "|1_kids_tags_shaoerpingdao|movie_comic|1_kids_tags_qingzi|1_kids_tags_yizhi|1_kids_tags_dongwu|1_kids_tags_tonghua" +
    "|1_kids_tags_yuer)-?").r
  def main(args: Array[String]) {
    ModuleClass.executor(sportsLiveDurationInfo,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val medusaDir = "/log/medusaAndMoretvMerger"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))


        (0 until p.numOfDays).foreach(i=>{
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date,-1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          val playDir=s"$medusaDir/$date/playview"
          sqlContext.read.parquet(playDir).registerTempTable("log")
          val rdd=sqlContext.sql("select flag,userId,path,pageDetailInfoFromPath from log where path like '%kids%' or " +
            "pathMain like '%kids%' and event in ('startplay','playview')").map(e=>(e.getString(0),e.getString(1),e.getString(2),e.getString(3))).
            persist(StorageLevel.MEMORY_AND_DISK)
          val medusaPlayRdd = rdd.filter(_._1=="medusa").filter(_._4!=null).
            filter(e=>{e._4.contains("kids_anim*") || e._4.contains("kids_rhymes*")}).map(e=>(getFourthPageTab(e._4,e._1),e._2))

          val moretvPlayRdd = rdd.filter(_._1=="moretv").map(e=>(getFourthPageTab(e._3,e._1),e._2)).filter(_._1!=null)

          val mergerPlayRdd = medusaPlayRdd.union(moretvPlayRdd)

          val numRdd = mergerPlayRdd.map(e=>(e._1,1)).reduceByKey(_+_)
          val userRdd = mergerPlayRdd.distinct().map(e=>(e._1,1)).reduceByKey(_+_)
          val mergerRdd = numRdd.join(userRdd)



          /*数据库操作*/
          if(p.deleteOld){
            val deleteSql = "delete from medusa_channel_eachtab_play_kids_info where day = ?"
            util.delete(deleteSql,insertDate)
          }
          val sqlInsert = "insert into medusa_channel_eachtab_play_kids_info(day,channelname,tabname,play_num,play_user) " +
            "values (?,?,?,?,?)"

          mergerRdd.collect().foreach(e=>{
            util.insert(sqlInsert,insertDate,"少儿",e._1,new JLong(e._2._1),new JLong(e._2._2))
          })
        })

      }
      case None => {throw new RuntimeException("At least needs one param: startDate!")}
    }
  }

  def getFourthPageTab(str:String,flag:String)={
    if(str!=null){
      if(flag=="medusa"){
        str.split("\\*")(1)
      }else if(flag=="moretv"){
        regex findFirstMatchIn str match {
          case Some(p)=>{
            p.group(2) match {
              case "kids_seecartoon" => {
                p.group(3) match {
                  case "search"=> "搜索"
                  case "kids_star" => "动画明星"
                  case "p_shaoer_hot_1" => "少儿热播"
                  case "kid_zhuanti" => "少儿专题"
                  case "zongyi_shaoer" => "少儿综艺"
                  case "p_donghua1" => "2-5岁"
                  case "p_donghua2" => "5-8岁"
                  case "p_donghua3" => "8-12岁"
                  case "kids-english" => "英文动画"
                  case "1_kids_tags_shaoerpingdao" => "少儿动画"
                  case "movie_comic" => "动画电影"
                  case "1_kids_tags_qingzi" => "亲子交流"
                  case "1_kids_tags_yizhi" => "益智启蒙"
                  case "1_kids_tags_dongwu" => "动物乐园"
                  case "1_kids_tags_tonghua" => "童话故事"
                  case "1_kids_tags_yuer" => "少儿教育"
                  case _=> null
                }
              }
              case "kids_songhome" => "2.X听儿歌"
              case _ =>null
            }
          }
          case None => null
        }
      }else null
    }else null
  }

}
