package com.moretv.bi.util

/**
 * Created by xiajun on 2016/6/29.
 */
object SubjectPathParserUtil {
  private var subjectCode=""
  private var homeAccessArea=""
  private val regexHomeAccessAreaMedusa="home\\*(recommendation|my_tv|classification).+".r


  def getSubjectCodeMedusa(str:String):String={
    if(isSubjectMedusa(str)){
      subjectCode=str.split("-")(1)
    }
    subjectCode
  }



  def isSubjectMedusa(str:String):Boolean={
    var res=false
    if(str!=null){
      if(str.contains("subject-")) res=true
    }
    res
  }
  def isSubjectMoretv(str:String):Boolean={
    var res=false
    if(str!=null){

    }
    res
  }
}
