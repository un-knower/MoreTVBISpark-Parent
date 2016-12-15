package com.moretv.bi.content

import java.lang.{Long => JLong}
import java.util.Calendar

import com.moretv.bi.util._
import com.moretv.bi.util.baseclasee.{ModuleClass, BaseClass}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Administrator on 2016/3/1.
 */
object SubjectChannelClassifiedStatistics extends BaseClass{
  def main(args: Array[String]) {
    ModuleClass.executor(SubjectChannelClassifiedStatistics,args)
  }
  override def execute(args: Array[String]) {
    ParamsParseUtil.parse(args) match{
      case Some(p)=>
        val util = DataIO.getMySqlOps(DataBases.MORETV_BI_MYSQL)
        val cal = Calendar.getInstance()
        cal.setTime(DateFormatUtils.readFormat.parse(p.startDate))

        (0 until p.numOfDays).foreach(i => {
          val date = DateFormatUtils.readFormat.format(cal.getTime)
          val day = DateFormatUtils.toDateCN(date,-1)
          val pathDetail = "/mbi/parquet/detail-subject/"+date+"/*"
          val pathPlayview = "/mbi/parquet/playview/"+date+"/*"
          val s = sqlContext
          import s.implicits._
          val df_detail = sqlContext.read.parquet(pathDetail).filter($"path".startsWith("home-subject-")).select("path","userId").cache()
          val df_playview = sqlContext.read.parquet(pathPlayview).filter($"path".startsWith("home-subject-")).select("path","userId").cache()
          val df_pv = df_detail.groupBy("path").count()
          val df_uv = df_detail.distinct().groupBy("path").count()
          val df_vv = df_playview.groupBy("path").count()
          val df_userPlay = df_playview.distinct().groupBy("path").count()

          val res_access = df_pv.join(df_uv,"path").map(row => {
            val path = row.getString(0)
            val pathCH = path2CH(path)
            if(pathCH != ""){
              (pathCH,(row.getLong(1),row.getLong(2)))
            }else null
          }).filter(_ != null).reduceByKey((x,y) => (x._1+y._1, x._2+y._2))

          val res_play = df_vv.join(df_userPlay,"path").map(row => {
            val path = row.getString(0)
            val pathCH = path2CHforPlayview(path)
            if(pathCH != ""){
              (pathCH,(row.getLong(1),row.getLong(2)))
            }else null
          }).filter(_ != null).reduceByKey((x,y) => (x._1+y._1, x._2+y._2))

          if(p.deleteOld){
            val sqlDelete = "Delete from SubjectChannelClassifiedStatistics where day = ?"
            util.delete(sqlDelete,day)
          }
          val sqlInsert = "INSERT INTO SubjectChannelClassifiedStatistics(day, TabName, access_num, user_num, play_num, userPlay_num) VALUES(?,?,?,?,?,?)"

          res_access.fullOuterJoin(res_play).collect.foreach(x => {
            util.insert(sqlInsert,day,x._1,new JLong(x._2._1.getOrElse((0L,0L))._1),new JLong(x._2._1.getOrElse((0L,0L))._2),
              new JLong(x._2._2.getOrElse((0L,0L))._1),new JLong(x._2._2.getOrElse((0L,0L))._2))
          })
          df_detail.unpersist()
          df_playview.unpersist()
          cal.add(Calendar.DAY_OF_MONTH, -1)
        })
        util.destory()
      case None => throw new RuntimeException("At least need param --startDate.")
    }
  }

  def path2CH(path:String) : String = {
    path match {
      case "home-subject-movie_zhuanti" => "电影专题"
      case "home-subject-tv_zhuanti" => "电视剧专题"
      case "home-subject-zongyi_zhuanti" => "综艺专题"
      case "home-subject-comic_zhuanti" => "动漫专题"
      case "home-subject-kid_zhuanti" => "少儿专题"
      case "home-subject-hot_zhuanti" => "资讯专题"
      case "home-subject-jishi_zhuanti" => "纪实专题"
      case "home-subject-movie_star" => "影人专区"
      case "home-subject-movie_xilie" => "系列电影"
      case "home-subject-tv_meizhouyixing" => "追星专题"
      case "home-subject-zongyi_weishi" => "卫视强档"
      case "home-subject-comic_dashi" => "动画大师"
      case _ => ""
    }
  }
  def path2CHforPlayview(path:String) : String = {
    path match {
      case _ if(path.startsWith("home-subject-movie_zhuanti") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "电影专题"
      case _ if(path.startsWith("home-subject-tv_zhuanti") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "电视剧专题"
      case _ if(path.startsWith("home-subject-zongyi_zhuanti") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "综艺专题"
      case  _ if(path.startsWith("home-subject-comic_zhuanti") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "动漫专题"
      case  _ if(path.startsWith("home-subject-kid_zhuanti") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "少儿专题"
      case  _ if(path.startsWith("home-subject-hot_zhuanti") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "资讯专题"
      case  _ if(path.startsWith("home-subject-jishi_zhuanti") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "纪实专题"
      case  _ if(path.startsWith("home-subject-movie_star") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "影人专区"
      case  _ if(path.startsWith("home-subject-movie_xilie") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "系列电影"
      case  _ if(path.startsWith("home-subject-tv_meizhouyixing") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "追星专题"
      case  _ if(path.startsWith("home-subject-zongyi_weishi") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "卫视强档"
      case  _ if(path.startsWith("home-subject-comic_dashi") && !(path.contains("-peoplealsolike")||path.contains("-similar")||path.contains("-tag")||
        path.contains("-actor"))) => "动画大师"
      case _ => ""
    }
  }
}
