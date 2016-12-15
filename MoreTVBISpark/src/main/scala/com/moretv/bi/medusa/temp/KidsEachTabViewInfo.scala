package com.moretv.bi.medusa.temp

import java.util.Calendar
import java.lang.{Long => JLong}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}


/**
  * Created by witnes on 8/22/16.
  * 统计少儿频道各列表页(三级目录)的播放量
  */
object KidsEachTabViewInfo extends BaseClass{

  private val tableName = "medusa_channel_eachtabwithgroup_view_kids_info"
  private val selectSql = "select userId, pathMain from log where pathMain like '%kids_home%'"


  private val regex_collect = ("(kids_collect)*(观看历史|收藏追看|专题收藏)").r
  private val regex_kandonghua = ("(kandonghua|kids_anim)*(动画明星|热播推荐|最新出炉|动画专题|欧美精选|国产精选|0-3岁|" +
    "4-6岁|7-10岁|英文动画|少儿电影|儿童综艺|亲子交流|益智启蒙|童话故事|教育课堂|搞笑|机战|亲子|探险)").r
  private val regex_tingerge = ("(tingerge|kids_rhymes)*(随便听听|儿歌明星|儿歌热播|儿歌专辑|英文儿歌|舞蹈律动)").r
  private val regex_xuezhishi = ("(xuezhishi)*(4-7岁幼教|汉语学堂|英语花园|数学王国|安全走廊|百科世界|艺术宫殿)").r


  /*删除指定一天的所有记录*/
  private val deleteSql = s"delete from $tableName where day = ?"

  private val insertSql = s"insert into $tableName(day, channelname, groupname,tabname,view_num,view_user)" +
  "values (?,?,?,?,?,?)"

  def main(args: Array[String]): Unit = {
    println("com.moretv.bi.medusa.temp.KidsEachTabViewInfo")
    ModuleClass.executor(KidsEachTabViewInfo, args)
  }

  override def execute(args: Array[String]): Unit = {
    ParamsParseUtil.parse(args) match {
      case Some(p) => {

        val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)
        val startDate = p.startDate
        val medusaBaseDir = "/log/medusa/parquet"
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(startDate))

        (0 until p.numOfDays).foreach(i=>{

          /*确定指定日期的数据源*/
          val date = DateFormatUtils.readFormat.format(calendar.getTime)
          val insertDate = DateFormatUtils.toDateCN(date, -1)
          calendar.add(Calendar.DAY_OF_MONTH,-1)
          val enterUserIdDate = DateFormatUtils.readFormat.format(calendar.getTime)

          /*拼接parquet数据源的完整路径*/
          val playDir =s"$medusaBaseDir/$date/tabview"

          /*获取 tabview DataFrame */
          sqlContext.read.parquet(playDir).registerTempTable("log")

          //少儿频道二级页面下统计数据
          var df =  sqlContext.sql("select userId, pathMain, stationcode from log where pathMain like '%kids_home%'")

          //create RDD[userId, pathMain+stationcode]
          val rdd = df.map(e=>(e.getString(0),e.getString(1)+e.getString(2)))

          //transform to RDD[userId, GroupName->TabName]
          val trans_rdd = rdd.map(e=>(e._1,matchGroup(e._2))).filter(_._2 != null)

          trans_rdd.cache()

          //create RDD[GroupName->TabName, view_num]
          val pv_rdd = trans_rdd.map(e=>(e._2,1)).reduceByKey(_+_)
          //create RDD[GroupName->TabName, view_user]
          val uv_rdd = trans_rdd.distinct().map(e=>(e._2,1)).reduceByKey(_+_)
          //create RDD[GroupName->TabName, view_num, view_user]
          val merge_rdd = pv_rdd.join(uv_rdd)

          /*将统计结果集插入Mysql */
          if(p.deleteOld){
            util.delete(deleteSql,insertDate)
          }

          merge_rdd.collect().foreach(e=>{
            //day | channelname | groupname | tabname | view_num | view_user
            println(insertDate, e._1.split("->")(0),e._1.split("->")(1), e._2._1, e._2._2)
            try {
              util.insert(insertSql,insertDate,"少儿",e._1.split("->")(0),e._1.split("->")(1),
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


  def matchGroup(str1:String):String = {

    regex_collect findFirstMatchIn str1 match {
      case Some(p) =>{
        p.group(1) match {
          case "kids_collect" => "猫猫屋"+"->" + p.group(2)
          case _ => null
        }
      }
      case None => {
        regex_collect findFirstMatchIn str1 match {
          case Some(p) =>  "猫猫屋" + "->"+ p.group(2)
          case None => {
            regex_kandonghua findFirstMatchIn str1 match {
              case Some(p) => "看动画" + "->" + p.group(2)
              case None => {
                regex_tingerge findFirstMatchIn str1 match {
                  case Some(p) => "听儿歌" + "->" + p.group(2)
                  case None => {
                    regex_xuezhishi findFirstMatchIn str1 match {
                      case Some(p) => "学知识" + "->" +p.group(2)
                      case None => null
                    }
                  }
                }
              }
            }
          }
        }
      }
//         regex_kandonghua findFirstMatchIn str1 match  {
//           case Some(p) =>{
//             p.group(1) match {
//               case "kandonghua"|"kids_anim" => "看动画" + "->"+ p.group(2)
//             }
//           }
//           case _ => {
//             regex_tingerge findFirstMatchIn str1 match {
//               case Some(p) =>{
//                 p.group(1) match {
//                   case "tingerge"|"kids_rhymes" => "听儿歌" + "->"+ p.group(2)
//                 }
//               }
//               case _ => {
//                 regex_xuezhishi findFirstMatchIn str1 match {
//                   case Some(p) =>{
//                     p.group(1) match {
//                       case "xuezhishi" => "学知识" + "->"+p.group(2)
//                     }
//                   }
//                   case _ => null
//                 }
//               }
//             }
//           }
//         }
//      }

      }

    }


}
