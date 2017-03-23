package com.moretv.bi.report.medusa.channeAndPrograma.mv

import java.util.Calendar
import java.lang.{Long => JLong}
import com.moretv.bi.util.{DBOperationUtils, DateFormatUtils, ParamsParseUtil}
import cn.whaley.sdk.dataexchangeio.DataIO
import com.moretv.bi.global.{DataBases, LogTypes}
import cn.whaley.sdk.dataOps.MySqlOps
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}

/**
  * Created by witnes on 9/20/16.
  */

/**
  * 浏览页统计 (指定tab)
  * 数据源: tabview
  * 特征提取: pathMain, stationcode, userId
  * 过滤条件: 分类, 演唱会, 舞蹈
  * 统计: pv, uv
  * 输出: tbl:[mv_tabview_pv_uv](day, entrance, tabname, uv, pv)
  */

@deprecated
object MVTabViewStat extends BaseClass{

  private val tableName = "mv_tabs_view_statistic"

  private val insertSql = s"insert into $tableName (day,entrance,tabname,uv,pv) values(?,?,?,?,?)"

  private val deleteSql = s"delete from $tableName where day = ? "

  private val selectedTabRegex = (
                                    "(mv\\*mvCategoryHomePage|" +
                                    "mv\\*function\\*site_concert|" +
                                    "mv\\*function\\*site_dance)"
                                  ).r

  def main(args: Array[String]) {

    ModuleClass.executor(this,args)

  }

  override def execute(args: Array[String]): Unit = {

      ParamsParseUtil.parse(args) match {

        case Some(p) => {

          // init & util
          val util = DataIO.getMySqlOps(DataBases.MORETV_MEDUSA_MYSQL)

          val startDate = p.startDate

          val cal = Calendar.getInstance

          cal.setTime(DateFormatUtils.readFormat.parse(startDate))

          (0 until p.numOfDays).foreach(w =>{

            //date
            val loadDate = DateFormatUtils.readFormat.format(cal.getTime)

            cal.add(Calendar.DAY_OF_MONTH,-1)

            val sqlDate = DateFormatUtils.cnFormat.format(cal.getTime)

            //df
            val df =
              DataIO.getDataFrameOps.getDF(sc,p.paramMap,MEDUSA,LogTypes.TABVIEW,loadDate)
                    .select("pathMain","stationcode","userId")
                    .filter("pathMain is not null")
                    .filter("stationcode is not null")

            //rdd
            val rdd =df.map(
                            e=>(
                                (filterTab(e.getString(0)),e.getString(1)),e.getString(2)
                                )
                            )
                        .filter(_._1._1 !=null)
                        .cache

            //aggregate

            val uvMap = rdd.distinct.countByKey

            val pvMap = rdd.countByKey

            //deal with table
            if(p.deleteOld){

              util.delete(deleteSql,sqlDate)

            }

            uvMap.foreach(w=>{

              val key = w._1

              val pv = pvMap.get(key) match {

                case Some(p) => p
                case None => 0

              }

              util.insert(
                insertSql, sqlDate,w._1._1,w._1._2,new JLong(w._2), new JLong(pv)
              )

            })

          })
        }
        case None =>{

          throw new Exception("MVTabViewStat fails")

        }
      }
  }

  /**
    *
    * @param field: pathMain
    * @return tabName
    */
  def filterTab(field:String):String ={

    selectedTabRegex findFirstMatchIn field match {

      case Some(p) =>{

        p.group(1) match {

          case "mv*mvCategoryHomePage" => "分类"

          case "mv*function*site_concert" => "演唱会"

          case "mv*function*site_dance" => "舞蹈"

          case _ => null

        }

      }

      case None => null
    }
  }
}
