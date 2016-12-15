package com.moretv.bi.medusa.temp

import java.util.Calendar
import java.lang.{Long => JLong}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by witnes on 8/22/16.
  * 统计少儿频道各列表页(三级目录)的播放量
  */
object KidsEachTabPlayInfo extends BaseClass {


  private val tableName = "medusa_channel_eachtabwithgroup_play_kids_info"
  private val selectSql = "select userId, pathMain from log where pathMain like '%kids_home%'"

  private val regex_tab = ("(kids_collect|kandonghua|tingerge|xuezhishi)\\*(观看历史|收藏追看|专题收藏|动画明星|热播推荐|" +
    "最新出炉|动画专题|欧美精选|国产精选|0-3岁启蒙|0-3岁|4-6岁|7-10岁|英文动画|少儿电影|儿童综艺|亲子交流|益智启蒙|童话故事|" +
    "教育课堂|随便听听|儿歌明星|儿歌热播|儿歌专辑|英文儿歌|舞蹈律动|热播推荐|4-7岁幼教|汉语学堂|英语花园|数学王国|安全走廊|" +
    "百科世界|艺术宫殿|搞笑|机战|亲子|探险)").r

  private val regex_search = ("(kids_collect-search|kandonghua-search|xuezhishi-search|" +
    "tingerge-search)\\*([0-9A-Za-z]+)").r

  /*删除指定一天的所有记录*/
  private val deleteSql = s"delete from $tableName where day = ?"

  private val insertSql = s"insert into $tableName(day, channelname, groupname,tabname,play_num,play_user)" +
    "values (?,?,?,?,?,?)"

  def main(args: Array[String]): Unit = {
    println("com.moretv.bi.medusa.temp.KidsEachTabPlayInfo")
    ModuleClass.executor(KidsEachTabPlayInfo, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i => {

          /*确定指定日期的数据源*/
          val loadDate = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(loadDate, -1)
          calendar.add(Calendar.DAY_OF_MONTH, -1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          /*拼接parquet数据源的完整路径*/
          val playDir = s"/log/medusaAndMoretvMerger/$loadDate/playview"

          /*获取 play DataFrame */
          sqlContext.read.parquet(playDir)
            .filter("event in ('startplay','playview')")
            .registerTempTable("log")

          //少儿频道二级页面下统计数据
          var df = sqlContext.sql("select userId, pathMain from log where pathMain like '%kids_home%'")

          //create RDD[userId, pathMain]
          val rdd = df.map(e => (e.getString(0), e.getString(1)))

          //transform to RDD[userId, GroupName-TabName]
          val trans_rdd = rdd.map(e => (e._1, matchRegex(e._2))).filter(_._2 != null)

          trans_rdd.cache()

          //create RDD[GroupName-TabName, play_num]?
          val pv_rdd = trans_rdd.map(e => (e._2, 1)).reduceByKey(_ + _)

          //create RDD[GroupName-TabName, play_user]?
          val uv_rdd = trans_rdd.distinct().map(e => (e._2, 1)).reduceByKey(_ + _)

          //create RDD[GroupName-TabName,[play_num,play_user]]
          val merge_rdd = pv_rdd.join(uv_rdd)

          //merge_rdd.collect().foreach(e=> println(e._1, e._2))

          /*将统计结果集插入Mysql */
          if (p.deleteOld) {
            util.delete(deleteSql, insertDate)
          }

          merge_rdd.collect().foreach(e => {
            //day | channelname | groupname | tabname | play_num | play_user
            println(insertDate, e._1.split("->")(0), e._1.split("->")(1), e._2._1, e._2._2)
            try {
              util.insert(insertSql, insertDate, "少儿", e._1.split("->")(0), e._1.split("->")(1),
                new JLong(e._2._1), new JLong(e._2._2))
            } catch {
              case e: Exception => println(e)
            }
          })
        })

      }
      case None => {
        throw new RuntimeException("at least needs one param: startDate")
      }

    }
  }


  def matchRegex(str: String): String = {
    regex_tab findFirstMatchIn str match {
      case Some(p) => {
        p.group(1) match {
          case "kids_collect" => "猫猫屋" + "->" + p.group(2)
          case "kandonghua" => "看动画" + "->" + p.group(2)
          case "tingerge" => "听儿歌" + "->" + p.group(2)
          case "xuezhishi" => "学知识" + "->" + p.group(2)
          case _ => null
        }
      }
      case None => {
        regex_search findFirstMatchIn str match {
          case Some(p) => {
            p.group(1) match {
              case "kandonghua-search" => "看动画" + "->" + "搜一搜"
              case "tingerge-search" => "听儿歌" + "->" + "搜一搜"
              case "xuezhishi-search" => "学知识" + "->" + "搜一搜"
              case _ => null
            }
          }
          case None => null
        }
      }
    }
  }


}
