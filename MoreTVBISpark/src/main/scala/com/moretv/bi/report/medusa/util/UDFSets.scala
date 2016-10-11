package com.moretv.bi.report.medusa.util

/**
 * Created by Xiajun on 2016/5/5.
 */
object UDFSets {
  /*该函数用于获取medusa的path中各级路径中的信息*/
  /**
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
              result = getZiduanInfo(indexPath,subIndex)
            }
          }
        }else{
          //直接获取第一级路径的信息
          if(x.contains("-")){
            val firstPath = x.split("-")(0)
            result = getZiduanInfo(firstPath,subIndex)
          }else{
            //只有一级路径
            result = getZiduanInfo(x,subIndex)
          }
        }
      }
      result
    }


  /**
   * 该函数用于解析medusa与moretv的pathMain、pathSub、specialPath、path
   * output：medusa统一格式的输出结果或者字段
   */
  def getMetaInfoFromPath(path:String,pathFlag:String,outInfoType:String)={
    var result:String =null
    if (path!=null){
      pathFlag match {
        //        处理medusa的pathMain信息
        case GlobalInfo.PATHFIELDS.PATHMAIN=> {
          outInfoType match {
            case GlobalInfo.OUTPUTFIELDS.LAUNCHERACCESSAREA => {
              result = pathInfoParser(path,1,2)
              if (!GlobalInfo.LAUNCHERACCESSAREA.contains(result) && result != null){
                // 这里处理的是home*(非访问区域的数据)%的情况
                result = null
              }else if(result==null && pathInfoParser(path,2,1)=="search"){
                // 这里处理的是home-search*%的情况
                result = "navi"
              }
            }
            case GlobalInfo.OUTPUTFIELDS.LAUNCHERACCESSLOCATION => {
              result = pathInfoParser(path,1,3)
              if (!GlobalInfo.ACCESSLOCATION.contains(result) && result != null){
                // 这里处理的是home*访问区域*(非正常的访问位置)%的情况
                result = null
              }else if(result==null && pathInfoParser(path,2,1)=="search"){
                // 这里处理的是
                result = "search"
              }
            }
            case GlobalInfo.OUTPUTFIELDS.PAGETYPE => {
              result = pathInfoParser(path,2,1)
              // 这里处理了“少儿”的特殊情况
              if (result=="kids_home"){
                result = pathInfoParser(path,3,1)
              }
              if (!GlobalInfo.PAGETYPE.contains(result) && result!=null){
                result = null
              }
            }
            case GlobalInfo.OUTPUTFIELDS.PAGETABINFO => {
              result = pathInfoParser(path,2,2)
              if(pathInfoParser(path,2,1)=="kids_home"){
                result = pathInfoParser(path,3,2)
              }
              if (pathInfoParser(path,2,1)=="search"){
                result = null
              }
            }
          }
        }
        //        处理medusa的pathSub信息
        case GlobalInfo.PATHFIELDS.PATHSUB => {
          outInfoType match {
            case GlobalInfo.OUTPUTFIELDS.ACCESSPATH => {
              result = getPathMainInfo(path,1,1)
            }
/*            case GlobalInfo.OUTPUTFIELDS.CURRENTSID =>{
              result = getPathMainInfo(path,2,1)
            }*/
            case GlobalInfo.OUTPUTFIELDS.UPSID => {
              result = getPathMainInfo(path,3,1)
            }
            case GlobalInfo.OUTPUTFIELDS.UPCONTENTTYPE => {
              result = getPathMainInfo(path,3,2)
            }
          }
        }
        //        处理medusa的pathSpecial信息
        case GlobalInfo.PATHFIELDS.PATHSPECIAL => {
          outInfoType match {
            case GlobalInfo.OUTPUTFIELDS.PATHPROPERTY => {
              result = getPathMainInfo(path,1,1)
            }
            case GlobalInfo.OUTPUTFIELDS.PATHFLAG => {
              result = getPathMainInfo(path,2,1)
            }
          }
        }
        //        处理moretv的path信息
        case GlobalInfo.PATHFIELDS.PATH=>{
//          Moretv的路径都是按照XXX-XXX-XXX-XXX的格式进行定义！
          if(path.contains("-")){
            outInfoType match {
              case GlobalInfo.OUTPUTFIELDS.LAUNCHERACCESSAREA => {
                result = getMoreTVSplitInfo(path,2)
                if (GlobalInfo.MORETVNAVI.contains(result)){
                  result = "navi"
                }else if(GlobalInfo.MORETVCLASSIFICATION.contains(result)){
                  result = "classification"
                }
              }
              case GlobalInfo.OUTPUTFIELDS.LAUNCHERACCESSLOCATION => {
                result = getMoreTVSplitInfo(path,2)
              }
              case GlobalInfo.OUTPUTFIELDS.PAGETYPE => {
                if(GlobalInfo.CONTENTTYPE.contains(getMoreTVSplitInfo(path,2))){
                  result = getMoreTVSplitInfo(path,2)
                }else if(getMoreTVSplitInfo(path,3)=="hotrecommend"){
                  result = getMoreTVSplitInfo(path,5)
                }else if(getMoreTVSplitInfo(path,2)=="watchhistory"){
                  result = getMoreTVSplitInfo(path,3)
                }else if(getMoreTVSplitInfo(path,2)=="otherwatch"){
                  result = getMoreTVSplitInfo(path,3)
                }else if(getMoreTVSplitInfo(path,2)=="kids_home"){
                  result = getMoreTVSplitInfo(path,3)
                }
              }
              case GlobalInfo.OUTPUTFIELDS.PAGETABINFO => {
                if(GlobalInfo.MORETVPAGETABINFOFOUR.contains(getMoreTVSplitInfo(path,2))){
                  result = getMoreTVSplitInfo(path,4)
                }else if(GlobalInfo.MORETVPAGETABINFOTHREE.contains(path,2)){
                  result = getMoreTVSplitInfo(path,3)
                }
              }

              case GlobalInfo.OUTPUTFIELDS.ACCESSPATH => {
                val len = path.split("-").length
                result = getMoreTVSplitInfo(path,len-1)
                if(!GlobalInfo.MORETVPATHSUBCATEGORY.contains(result)){
                  result = null
                }
              }
//
              case GlobalInfo.OUTPUTFIELDS.UPSID => {
                val len = path.split("-").length
                result = getMoreTVSplitInfo(path,len-1)
                if(GlobalInfo.MORETVPATHSUBCATEGORY.contains(result)){
                  result = getMoreTVSplitInfo(path,len-2)
                }
              }
              case GlobalInfo.OUTPUTFIELDS.UPCONTENTTYPE => {
                val len = path.split("-").length
                result = getMoreTVSplitInfo(path,len-1)
                if(GlobalInfo.MORETVPATHSUBCATEGORY.contains(result)){
                  result = getMoreTVSplitInfo(path,len-3)
                }
              }
              case GlobalInfo.OUTPUTFIELDS.PATHPROPERTY => {
                if(path.contains("-tag-")){
                  result = "tag"
                }else if(path.contains("-subject-")){
                  result ="subject"
                }else if(path.contains("-actor-")){
                  result = "star"
                }
              }
              case GlobalInfo.OUTPUTFIELDS.PATHFLAG => {
                // 在moretv中没有给pathproperty添加具体的信息，即没有给出tag/subject/actor所对应的具体内容，因此设为null
                result = null
              }
            }
          }
        }
      }
    }
    result
  }


  def pathInfoParser(path:String,index:Int,subIndex:Int)={
    var result:String = null
    //过滤null的信息
    if(path!=null){
      //判断是需要哪一级路径
      if(index>=2){
        //获取非第一级的信息
        if(path.contains("-")){
          val splitData = path.split("-")
          if(splitData.length>=index){
            val indexPath = splitData(index-1)
            result = getZiduanInfo(indexPath,subIndex)
          }
        }
      }else{
        //直接获取第一级路径的信息
        if(path.contains("-")){
          val firstPath = path.split("-")(0)
          result = getZiduanInfo(firstPath,subIndex)
        }else{
          //只有一级路径
          result = getZiduanInfo(path,subIndex)
        }
      }
    }
    result
  }

  def getZiduanInfo(path:String,subIndex:Int)={
    var result:String = null
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
        result = path.split("\\*")(0)
      }else{
        result = path.toString
      }
    }
    result
  }

  def getMoreTVSplitInfo(path:String,index:Int)={
    var result:String = null
    val splitData = path.split("-")
    val len = splitData.length
    if(index<=len){
      result = splitData(index)
    }
    result
  }

}
