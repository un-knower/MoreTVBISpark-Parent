package com.moretv.bi.report.medusa.util.udf

/**
 * Created by Administrator on 2016/5/12.
 */
object LauncherAccessAreaParser {

  def launcherAccessAreaParser(area:String,app:String)={
    require(app!=null)
    var result:String = null
    app match {
      case "medusa" => {
        if (area!=null){
          area match {
            case "my_tv" => result="我的电视"
            case "recommendation" => result="今日推荐"
            case "foundation" => result="发现"
            case "classification" => result="分类"
            case "live" => result="直播"
            case "menu" => result="消息中心"
            case "navi" => result="搜索与设置"
            case _ => result="其他"
          }
        }
      }
      case "moretv" => {
        if(area!=null){
          area match {
            case "4" => result="观看历史"
            case "1" => result="热门推荐"
            case "2" => result="卫视直播"
            case "3"=> result="大家在看"
            case "5" => result="分类区域"
            case _ => result="其他"
          }
        }
      }
      case _ => {println("处理其他应用！")}
    }
    result
  }

  def launcherLocationIndexParser(locationIndex:String)={
    var result:String = null
      locationIndex match {
        case "1" => result = "第1个位置"
        case "2" => result = "第2个位置"
        case "3" => result = "第3个位置"
        case "4" => result = "第4个位置"
        case "5" => result = "第5个位置"
        case "6" => result = "第6个位置"
        case "7" => result = "第7个位置"
        case "8" => result = "第8个位置"
        case "9" => result = "第9个位置"
        case "10" => result = "第10个位置"
        case "11" => result = "第11个位置"
        case "12" => result = "第12个位置"
        case "13" => result = "第13个位置"
        case "14" => result = "第14个位置"
        case "15" => result = "第15个位置"
        case "16" => result = "第16个位置"
        case "17" => result = "第17个位置"
        case "18" => result = "第18个位置"
        case "19" => result = "第19个位置"
        case "20" => result = "第20个位置"
        case _ => result = "其他位置"
      }
    result
  }


  def launcherAccessLocationParser(accessLocation:String,app:String)={
    require(app!=null)
    var result:String = null
    app match {
      case "medusa" => {
        if (accessLocation!=null){
          accessLocation match {
            case "history" => result="历史"
            case "collect" => result="收藏"
            case "account" => result="我"
            case "site_live" => result="直播"
            case "site_tv" => result="电视"
            case "site_movie" => result="电影"
            case "site_kids" => result="少儿"
            case "site_hot" => result="资讯短片"
            case "site_sport" => result="体育"
            case "site_application" => result="应用推荐"
            case "site_jilu" => result="纪实"
            case "site_zongyi" => result="综艺"
            case "site_xiqu" => result="戏曲"
            case "site_mv" => result="音乐"
            case "site_comic" => result="动漫"
            case "top_star" => result="明星榜"
            case "top_collect" => result="收藏榜"
            case "top_hot" => result="热播榜"
            case "top_new" => result="新剧榜"
            case "interest_location" => result="兴趣坐标"
            case "live" => result="直播"
            case "tv" => result="电视"
            case "movie" => result="电影"
            case "kids" => result="少儿"
            case "hot" => result="资讯短片"
            case "sport" => result="体育"
            case "application" => result="应用推荐"
            case "jilu" => result="纪实"
            case "zongyi" => result="综艺"
            case "xiqu" => result="戏曲"
            case "comic" => result="动漫"
            case "mv" => result="音乐"
            case "setting" => result="设置"
            case "search" => result="搜索"
            case "messageCenter" => result="消息中心"
            case _ => result="其他"
          }
        }
      }
      case "moretv" => {println("处理Moretv的应用！")}
      case _ => {println("处理其他应用！")}
    }
    result
  }


  def moretvLauncherAccessLocationParser(area:String,accessLocation:String)={
    var result:String=null
    if(area!=null){
      if(accessLocation!=null){
        accessLocation match {
          case "0" => {
            if(area!="5"){
              result = "第1个位置"
            }else{
              result = "搜索"
            }}
          case "1" => {
            if(area!="5"){
              result = "第2个位置"
            }else{
              result ="历史收藏"
            }
          }
          case "2" => {
            if(area!="5"){
              result = "第3个位置"
            }else{
              result = "电影"
            }
          }
          case "3" => {
            if(area!="5"){
              result = "第4个位置"
            }else{
              result = "电视"
            }
          }
          case "4" => {
            if(area!="5"){
              result = "第5个位置"
            }else{
              result = "直播"
            }
          }
          case "5" => {
            if(area!="5"){
              result = "第6个位置"
            }else{
              result = "资讯短片"
            }
          }
          case "6" => {
            if(area!="5"){
              result = "第7个位置"
            }else{
              result = "综艺"
            }
          }
          case "7" => {
            if(area!="5"){
              result = "第8个位置"
            }else{
              result = "设置"
            }
          }
          case "8" => {
            if(area!="5"){
              result = "第9个位置"
            }else{
              result = "少儿"
            }
          }
          case "9" => {
            if(area!="5"){
              result = "第10个位置"
            }else{
              result = "动漫"
            }
          }
          case "10" => {
            if(area!="5"){
              result = "第11个位置"
            }else{
              result = "音乐"
            }
          }
          case "11" => {
            if(area!="5"){
              result = "第12个位置"
            }else{
              result = "纪实"
            }
          }
          case "12" => {
            if(area!="5"){
              result = "第13个位置"
            }else{
              result = "戏曲"
            }
          }
          case "13" => {
            if(area!="5"){
              result = "第14个位置"
            }else{
              result = "体育"
            }
          }
          case "14" => {
            if(area!="5"){
              result = "第15个位置"
            }else{
              result ="专题"
            }
          }
          case "15" => result = "第16个位置"
          case "16" => result = "第17个位置"
          case "17" => result = "第18个位置"
          case "18" => result = "第19个位置"
          case "19" => result = "第20个位置"
          case _ => result = "其他位置"
        }
      }
    }
    result
  }

}
