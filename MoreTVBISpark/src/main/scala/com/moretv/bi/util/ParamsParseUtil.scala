package com.moretv.bi.util

import java.text.SimpleDateFormat

import scopt.OptionParser

/**
 * Created by Will on 2015/9/28.
 */
object ParamsParseUtil {

  private val default = Params()
  private val readFormat = DateFormatUtils.readFormat
  private val timeFormat = new SimpleDateFormat("HH:mm:ss")

  def parse(args: Seq[String],default:Params = default) = {
      if(args.nonEmpty){
        val parser = new OptionParser[Params]("ParamsParse") {
          head("ParamsParse","1.2")
          opt[Map[String,String]]("paramMap").valueName("k1=v1,k2=v2...").action((x,c)=>c.copy(paramMap=x)).
            text("param Map[String,String]")
          opt[String]("startDate").action((x,c)=>c.copy(startDate=x)).
            validate(e => try {
              readFormat.parse(e)
              success
            }catch {
              case e:Exception => failure("wrong date format, should be 'yyyyMMdd'")
            })
          opt[Boolean]("deleteOld").action((x,c)=>c.copy(deleteOld = x))
          opt[String]("apkVersion").action((x,c)=>c.copy(apkVersion = x))
          opt[String]("columns").action((x,c)=>c.copy(columns=x))
          opt[String]("logTypes").action((x,c)=>c.copy(logTypes=x))
          opt[String]("logType").action((x,c)=>c.copy(logType=x))
          opt[String]("startDate").action((x,c)=>c.copy(startDate=x))
          opt[String]("srcPath").action((x,c)=>c.copy(srcPath=x))
//          opt[String]("statXml").action((x,c)=>c.copy(statXml=x))
          opt[String]("outputFile").action((x,c)=>c.copy(outputFile=x))
//          opt[String]("database").action((x,c)=>c.copy(database=x))
//          opt[String]("contentType").action((x,c)=>c.copy(contentType=x))
          opt[String]("whichMonth").action((x,c)=>c.copy(whichMonth=x))
          opt[Boolean]("alarmFlag").action((x,c)=>c.copy(alarmFlag = x))
          opt[Int]("durationMax").action((x,c)=>c.copy(durationMax=x))
          opt[Int]("numOfDays").action((x,c)=>c.copy(numOfDays=x)).
            validate(e => {
              if(e > 0) success else failure("numOfDays must be bigger than 0")
            })
          opt[Int]("whichDay").action((x,c)=>c.copy(whichDay=x))
          opt[String]("endDate").action((x, c) => c.copy(endDate = x))
          opt[Int]("offset").action((x,c)=>c.copy(offset=x))
          //          opt[String]("startTime").action((x,c)=>c.copy(startTime=x)).
          //            validate(e => try {
          //              timeFormat.parse(e)
          //              success
          //            }catch {
          //              case e:Exception => failure("wrong time format, should be 'HH:mm:ss'")
          //            })
          //          opt[String]("endTime").action((x,c)=>c.copy(endTime=x)).
          //            validate(e => try {
          //              timeFormat.parse(e)
          //              success
          //            }catch {
          //              case e:Exception => failure("wrong time format, should be 'HH:mm:ss'")
          //            })

          //          opt[String]("sid").action((x,c)=>c.copy(sid=x))
//          opt[String]("dateInfo").action((x,c)=> c.copy(dateInfo=x))
        }
        parser.parse(args,default)
      }else Some(default)
  }

  def withParse(args:Seq[String])(f : Params => Unit) = {
    parse(args) match {
      case Some(p) => f(p)
      case None =>
    }
  }

}
