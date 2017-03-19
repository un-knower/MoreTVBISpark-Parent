package com.moretv.bi.report.medusa.util.udf

import com.moretv.bi.report.medusa.util.EnChConvert.transformEng2Chinese
import com.moretv.bi.report.medusa.util.MedusaSubjectNameCodeUtil
import com.moretv.bi.util.{CodeToNameUtils, SubjectUtils}


/**
 * Created by 夏俊 on 2016/5/11.
 */


object PathParser {

  /**
   * 该对象用于解析路径信息
   * 输入参数说明：
   *             logType:指定日志的类型
   *             path:指定日志中需要解析的内容
   *             path:指定日志中path的字段信息
   *             outType:指定解析出的内容
   * 输出说明：
   *             根据outType产生相应的输出结果
   */

  def pathParser(logType:String,path:String,pathType:String,outputType:String)={
    // 要求logType不为空
    require(logType!=null)

    var result:String = null
    // 根据不同的logType进行解析
    logType match {

      /**
       * logType: interview
       */
      case UDFConstant.INTERVIEW => {
        pathType match {
          // moretv的interview的日志只有path这一个路径信息
          case UDFConstant.PATH => {
            outputType match {
              // 获取moretv的contentType信息
              case UDFConstant.CONTENTTYPE => {
                if(path != null){
                  if(path.contains("-")){
                    if(path.split("-").length>=2){
                      result = path.split("-")(1)
                    }
                  }
                }
              }
              case _ => {println(logType+"-"+pathType+"-"+"处理其他outputType!")}
            }
          }
          case _ => {println(logType+"-"+pathType+"-"+"处理其他pathType!")}
        }
      }

      /**
       * logType: detail/play/playview
       */
      case UDFConstant.DETAIL | UDFConstant.PLAY | UDFConstant.PLAYVIEW => {
        pathType match {
            // medusa的detail日志中的pathMain字段
          case UDFConstant.PATHMAIN => {
            outputType match {
                // medusa的pathMain路径中的launcherArea信息
              case UDFConstant.LAUNCHERAREA => {
                result = getPathMainInfo(path,1,2)
                if(result!=null){
                  if(!UDFConstant.MedusaLauncherArea.contains(result)){
                    result = null
                  }
                }else{
                  if(getPathMainInfo(path,2,1)=="search" || getPathMainInfo(path,2,1)=="setting"){
                    result = "navi"
                  }
                }
              }
                // medusa的pathMain路径中的launcherAccessLocation信息
              case UDFConstant.LAUNCHERACCESSLOCATION => {
                result = getPathMainInfo(path,1,3)
                if(result!=null){
                  if(getPathMainInfo(path,1,2)==UDFConstant.MedusaLive || !UDFConstant.MedusaLauncherAccessLocation
                    .contains(result)){
                    result = null
                  }
                }else{
                  // 处理launcher的搜索和设置的点击事件
                  if(getPathMainInfo(path,2,1)=="search"){
                    result = "search"
                  }else if(getPathMainInfo(path,2,1)=="setting"){
                    result = "setting"
                  }
                }
              }
                // medusa的pathMain路径中的pageType信息
              case UDFConstant.PAGETYPE => {
                result = getPathMainInfo(path,2,1)
                if(result!=null){
                  if(!UDFConstant.MedusaPageInfo.contains(result)){
                    result = null
                  }
                }
              }
                // medusa的pathMain路径中的pageDetailInfo信息
              case UDFConstant.PAGEDETAILINFO => {
                result = getPathMainInfo(path,2,2)
                if(result!=null){
                  if(!UDFConstant.MedusaPageDetailInfo.contains(result)){
                    result = null
                  }
                }else{
                  // 处理少儿频道
                  if(getPathMainInfo(path,2,1)=="kids_home"){
                    result = getSplitInfo(path,3)
                    if(result!=null && !UDFConstant.MedusaPageDetailInfo.contains(result)){
                      result = null
                    }
                    if(getPathMainInfo(path,3,2)==null){
                      if(getPathMainInfo(path,4,1)=="search"){
                        result = "搜一搜"
                      }
                    }else if(getPathMainInfo(path,3,2)=="搜一搜"){
                      result="搜一搜"
                    }
                  }

                  // 处理搜索和筛选
                  if(getPathMainInfo(path,3,1)=="search" || getPathMainInfo(path,3,1)=="retrieval"){
                    result = getPathMainInfo(path,3,1)
                  }
                }
              }
            }
          }
            // medusa的detail/play日志中的pathSub字段
          case UDFConstant.PATHSUB => {
            outputType match {
                // medusa的pathSub字段中的访问路径信息
              case UDFConstant.ACCESSPATH => {
                result = getPathMainInfo(path,1,1)
                if(!UDFConstant.MedusaPathSubAccessPath.contains(result)){
                  result = null
                }
              }
                // medusa的pathSub字段中的前一个节目的sid
              case UDFConstant.PREVIOUSSID => {
                if(UDFConstant.MedusaPathSubAccessPath.contains(getPathMainInfo(path,1,1))){
                  result = getPathMainInfo(path,3,1)
                }
              }
                // medusa的pathSub字段中的前一个节目的contentType
              case UDFConstant.PREVIOUSCONTENTTYPE => {
                if(UDFConstant.MedusaPathSubAccessPath.contains(getPathMainInfo(path,1,1))){
                  result = getPathMainInfo(path,3,2)
                }
              }
            }
          }
            // medusa的detail/play日志中的pathSpecial字段
          case UDFConstant.PATHSPECIAL => {
            outputType match {
                // 获取medusa的pathSpecial中的“路径性质”信息
              case UDFConstant.PATHPROPERTY => {
                result = getPathMainInfo(path,1,1)
                if(!UDFConstant.MedusaPathProperty.contains(result)){
                  result = null
                }
              }
                // 获取medusa的pathSpecial中的“路径标识”信息
              case UDFConstant.PATHIDENTIFICATION => {
                if(UDFConstant.MedusaPathProperty.contains(getPathMainInfo(path,1,1))){
                  val pathLen = path.split("-").length
                  if(pathLen==2){
                    result = getPathMainInfo(path,2,1)
                  }else if(pathLen>2){
                    var tempResult = getPathMainInfo(path,2,1)
                    var splitData = path.split("-")
                    for(i<- 2 until pathLen){
                      tempResult = tempResult.concat("-").concat(getPathMainInfo(path,i+1,1))
                    }
                    result = tempResult
                  }
                }
              }
            }
          }
            // moretv的detail日志中的path字段
          case UDFConstant.PATH => {
            if(path.contains("-")){
              outputType match {
                  // moretv的launcherArea信息
                case UDFConstant.LAUNCHERAREA => {
                  result = getSplitInfo(path,2)
                  if(result!=null){
                    // 如果是属于“热门推荐”、“大家在看”，“卫视直播”，“观看历史”，则area信息就为其本身
                    if(!UDFConstant.MoretvLauncherUPPART.contains(result)){
                      if (UDFConstant.MoretvLauncherAreaNAVI.contains(result)){
                        result = "navi"                            // 包含了"搜索"和“设置”
                      }else if(UDFConstant.MoretvLauncherCLASSIFICATION.contains(result)){
                        result = "classification"                  // 包含了launcher页面的下面部分，除了"搜索"和“设置”之外
                      }
                    }
                  }
                }
                  // moretv的accessLocation信息
                case UDFConstant.LAUNCHERACCESSLOCATION => {
                  result = getSplitInfo(path,2)
                  if(result!=null){
                    // 如果accessArea为“navi”和“classification”，则保持不变，即在launcherAccessLocation中
                    if(!UDFConstant.MoretvLauncherAccessLocation.contains(result)){
                      // 如果不在launcherAccessLocation中，则判断accessArea是否在uppart中
                      if(UDFConstant.MoretvLauncherUPPART.contains(result)){
                        result match {
                          case "watchhistory" => result = null
                          case "otherwatch" => result = getSplitInfo(path,3)
                          case "hotrecommend" => result = getSplitInfo(path,3)
                          case "TVlive" => result = null
                          case _ => result = null
                        }
                      }else{
                        result = null
                      }
                    }
                  }
                }
                  // moretv的pageType信息
                case UDFConstant.PAGETYPE => {
                  if(UDFConstant.MoretvPageInfo.contains(getSplitInfo(path,2))){
                    result = getSplitInfo(path,2)
                  }
                }
                  // moretv的pageDetailInfo
                case UDFConstant.PAGEDETAILINFO => {
                  result = getSplitInfo(path,3)
                  if(result!=null){
                    if(getSplitInfo(path,2)=="search"){
                      result = null
                    }
                    if(getSplitInfo(path,2)=="kids_home" || getSplitInfo(path,2)=="sports"){
                      result = getSplitInfo(path,3)+"-"+getSplitInfo(path,4)
                    }
                  }
                  // 将English转为Chinese
                  if(UDFConstant.MoretvPageInfo.contains(getSplitInfo(path,2))){
                    val page = getSplitInfo(path,2)
                    if(UDFConstant.MoretvPageDetailInfo.contains(result)){
                      result = transformEng2Chinese(page,result)
                    }
                  }
                }

                  // 处理moretv的“访问路径”信息
                case UDFConstant.ACCESSPATH => {
                  var len = 0
                  if(path.contains("-")){
                    len = path.split("-").length
                  }else{
                    len = 1
                  }
                  result = getSplitInfo(path,len)
                  if(!UDFConstant.MORETVPATHSUBCATEGORY.contains(result)){
                    result = null
                  }
                }
                  //目前moretv的path字段中没有上一个节目的sid和contentType的信息
                case UDFConstant.PREVIOUSSID => {
                  result = null
                }
                case UDFConstant.PREVIOUSCONTENTTYPE => {
                  result = null
                }
                case UDFConstant.PATHPROPERTY => {
                  if(path.contains("-tag-")){
                    result = "tag"
                  }else if(path.contains("-subject-")){
                    result ="subject"
                  }else if(path.contains("-actor-")){
                    result = "star"
                  }
                }
                case UDFConstant.PATHIDENTIFICATION => {
                  // 在moretv中没有给pathproperty添加具体的信息，即没有给出actor所对应的具体内容，因此设为null；
                  // tag/subject/的信息可以给出
//                  if(path.contains("-subject-")){
//                    // 让subjectIndex从1开始表示
//                    val subjectIndex = path.split("-").indexOf("subject")+1
//                    if(subjectIndex==2){
//                      result = getSplitInfo(path,subjectIndex+2)
//                    }else {
//                      result = getSplitInfo(path,subjectIndex+1)
//                    }
//                  }else if(path.contains("-tag-")){
//                    // 取最后一次出现的tag的位置信息
//                    val tagIndex = path.split("-").lastIndexOf("tag")+1
//                    result = getSplitInfo(path,tagIndex+1)
//                  }
                  result = getSubjectCodeByPath(path,"moretv")
                }
              }
            }
          }
        }
      }
    }
    result
  }


  /**
   * 该函数用于获取moretv的subjectCode
   */
  def getSubjectCode(path:String)={
    var result:String = null
    if(path !=null){
      val regex = """^.*-([movie|tv|comic|zongyi|kids|hot|jilu]+[0-9]+).*$""".r
      path match {
        case regex(subjectCode) => result=subjectCode
        case _ =>
      }
    }
    result
  }



  /**
   * 该函数用于获取medusa的path中各级路径中的信息
   * index:用于指定一级、二级信息，在path中，一级、二级路径是通过"-"来分隔,index取值为：1,2,3...
   * subIndex:用于指定具体的某一级路径（如第一级）中的第几个字段的信息，不同字段的信息是通过"*"来分隔的,subIndex取值为：1,2,3...
   */

  def getPathMainInfo(x:String,index:Int,subIndex:Int)={
    var result:String = null
    //过滤null的信息
    if(x!=null){
      //判断是需要哪一级路径
      if(index>=2){
        //获取非第一级的信息
        if(x.contains("-")){
          val splitData = x.split("-")
          if(splitData.length>=index){
            val indexPath = splitData(index-1)
            result = getMedusaPathDetailInfo(indexPath,subIndex)
          }
        }
      }else{
        //直接获取第一级路径的信息
        if(x.contains("-")){
          if(x!="-"){
            val firstPath = x.split("-")(0)
            result = getMedusaPathDetailInfo(firstPath,subIndex)
          }
        }else{
          //只有一级路径
          result = getMedusaPathDetailInfo(x,subIndex)
        }
      }
    }
    result
  }

  /**
   * 该函数用于获取medusa的一级、二级、...中第index个索引的信息，各个索引之间是用“*”来分隔的
   * @param path:路径中“-”拆分出来的每一级路径信息
   * @param subIndex：路径中“-”拆分出来的每一级路径信息中，通过“*”拆分的第几个内容，从1,2，...开始
   * @return
   */
  def getMedusaPathDetailInfo(path:String,subIndex:Int)={
    var result:String = null

    if(path!=null) {
      if(subIndex>=2){
        /*所需信息为第一级路径中的其他字段信息*/
        if(path.contains("*")){
          if(path.split("\\*").length>=subIndex){
            result=path.split("\\*")(subIndex-1)
          }
        }
      }else{
        /*所需信息为第一级路径中的第一个字段信息*/
        if(path.contains("*")){
          if(path!="*"){
            try{
              result = path.split("\\*")(0)
            }catch{
              case e:Exception=>e.printStackTrace()
            }
          }
        }else{
          result = path.toString
        }
      }
    }
    result
  }

  /**
   * 该函数用于获取moretv的分隔信息，moretv的path是用“-”来分隔的,index从1开始
   * @param path
   * @param index
   * @return
   */
  def getSplitInfo(path:String,index:Int)={
    var result:String = null
    if(path !=null){
      if(index>=2){
        /*所需信息为第一级路径中的其他字段信息*/
        if(path.contains("-")){
          if(path.split("-").length>=index){
            result=path.split("-")(index-1)
          }
        }
      }else{
        /*所需信息为第一级路径中的第一个字段信息*/
        if(path.contains("-")){
          if(path != "-"){
            result = path.split("-")(0)
          }
        }else{
          result = path.toString
        }
      }
    }
    result
  }

    /**
     *  从路径中获取专题code
     */
  def getSubjectCodeByPath(path:String,flag:String) = {
    var result:String = null
    if(flag!=null){
      flag match {
        case "medusa" => {
          if(path!=null){
            if(path.contains("subject")){
              val subjectCode = MedusaSubjectNameCodeUtil.getSubjectCode(path)
              if(subjectCode!=" "){
                result = subjectCode
              }
            }
          }
        }
        case "moretv" => {
          if(path!=null){
            val info = SubjectUtils.getSubjectCodeAndPath(path)
            if(!info.isEmpty){
                val subjectCode = info(0)
                result = subjectCode._1
            }
          }
        }
        case _ =>
      }
    }
    result
  }

  /**
    *  从路径中获取专题code
    */
  def getSubjectCodeByPathETL(path:String,flag:String) = {
    var result:String = null
    if(flag!=null){
      flag match {
        case "medusa" => {
          if(path!=null){
            if(path.contains("subject")){
              val subjectCode = MedusaSubjectNameCodeUtil.getSubjectCode(path)
              if(subjectCode!=" "){
                result = subjectCode
              }
            }
          }
        }
        case "moretv" => {
          if(path!=null){
            val info = SubjectUtils.getSubjectCodeAndPath(path)
            if(!info.isEmpty){
              val subjectCode = info(0)
              result = subjectCode._1
            }
          }
        }
        case _ =>
      }
    }
    result
  }


  /**
    *  medusa:从pathSpecial路径中获取subject code,如果pathSpecial没有包含subject code,使用subject name去数据库查询获得subject code
    *         subject-新闻头条-hot11 -> hot11
    *         subject-儿歌一周热播榜  ->儿歌一周热播榜
    *  moretv：从path里获得subject code
    */
  def getSubjectCodeByPathETLOld(path:String,flag:String) = {
    var result:String = null
    if(flag!=null){
      flag match {
        case "medusa" => {
          if(path!=null){
            if(path.contains("subject")){
              val subjectCode = MedusaSubjectNameCodeUtil.getSubjectCode(path)
              if(subjectCode!=" "){
                result = subjectCode
              }else{
                //get subject code from mysql according subject name
                val subjectName=MedusaSubjectNameCodeUtil.getSubjectNameETL(path)
                if(null!=subjectName){
                  result=CodeToNameUtils.getSubjectCodeByName(subjectName)
                }
              }
            }
          }
        }
        case "moretv" => {
          if(path!=null){
            val info = SubjectUtils.getSubjectCodeAndPath(path)
            if(!info.isEmpty){
              val subjectCode = info(0)
              result = subjectCode._1
            }
          }
        }
        case _ =>
      }
    }
    result
  }

      /**
       *   从路径中获取专题名称
        */
  def getSubjectNameByPath(path:String,flag:String) = {
    var result:String = null
    if(flag!=null){
      flag match {
        case "medusa" => {
          if(path!=null){
            if(path.contains("subject")){
              val subjectCode = MedusaSubjectNameCodeUtil.getSubjectCode(path)
              val pathLen = path.split("-").length
              if(pathLen==2){
                result = getPathMainInfo(path,2,1)
              }else if(pathLen>2){
                var tempResult = getPathMainInfo(path,2,1)
                if(subjectCode!=" "){
                  for(i<- 2 until pathLen-1){
                    tempResult = tempResult.concat("-").concat(getPathMainInfo(path,i+1,1))
                  }
                  result = tempResult
                }else{
                  for(i<- 2 until pathLen){
                    tempResult = tempResult.concat("-").concat(getPathMainInfo(path,i+1,1))
                  }
                  result = tempResult
                }
              }
            }
          }
        }
        case "moretv" => {
          if(path!=null){
            val info = SubjectUtils.getSubjectCodeAndPath(path)
            if(!info.isEmpty){
                val subjectCode = info(0)
                result = CodeToNameUtils.getSubjectNameBySid(subjectCode._1)
            }
          }
        }
        case _ =>
      }
    }
    result
  }

  /**
    *从路径中获取专题名称,对于medusa日志，可以从pathSpecial解析出subjectName；对于moretv日志，日志里面不存在subjectName打点
    * @param pathSpecial  medusa play pathSpecial field
    * @return subject_name string value or null
    * Example:
    *
    * {{{
    *   sqlContext.sql("
    *   select pathSpecial,subjectName,subjectCode
    *   from log_data
    *   where flag='medusa' and pathSpecial is not null and size(split(pathSpecial,'-'))=2").show(100,false)
    * }}}

  * */
  def getSubjectNameByPathETL(pathSpecial:String) :String= {
    var result:String = null
    if(pathSpecial!=null){
      if(pathSpecial.contains("subject")){
        val subjectCode = MedusaSubjectNameCodeUtil.getSubjectCode(pathSpecial)
        val pathLen = pathSpecial.split("-").length
        if(pathLen==2){
          result = getPathMainInfo(pathSpecial,2,1)
        }else if(pathLen>2){
          var tempResult = getPathMainInfo(pathSpecial,2,1)
          if(subjectCode!=" "){
            for(i<- 2 until pathLen-1){
              tempResult = tempResult.concat("-").concat(getPathMainInfo(pathSpecial,i+1,1))
            }
            result = tempResult
          }else{
            for(i<- 2 until pathLen){
              tempResult = tempResult.concat("-").concat(getPathMainInfo(pathSpecial,i+1,1))
            }
            result = tempResult
          }
        }
      }
    }
    result
  }

  def getSubjectTypeByPathETL(path:String,flag:String) :String= {
    var result:String=null
    if(flag!=null && path!=null){
      flag match {
        case "medusa" => {
            result = getPathMainInfo(path,1,1)
            if(result.equalsIgnoreCase("subject")){
              result = "subject"
             }
          }
        case "moretv" => {
            if(path.contains("-subject-")){
              result ="subject"
            }
        }
        case _ =>
      }
    }
    result
  }

  def main(args: Array[String]) {
    //val pathSpecial="subject-儿歌一周热播榜"
    val pathSpecial="subject-六一儿歌行-kid8"
    println(getSubjectCodeByPathETL(pathSpecial,"medusa"))
    println(getSubjectNameByPathETL(pathSpecial))
  }
}

