package com.moretv.bi.util

/**
  * Created by Will on 2016/2/3.
  */
object SubjectUtils {

  //匹配首页上的专题
  val regexSubjectA = "home-(hotrecommend)(-\\d+-\\d+)?-(hot\\d+|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配首页上的专题套专题
  val regexSubjectA2 = ("home-(hotrecommend)(-\\d+-\\d+)?-(hot\\d+|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+)-"
    + "(actor|hot\\d+|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+)").r

  //匹配在三级页面的专题
  val regexSubjectB = "home-(movie|zongyi|tv|comic|kids|jilu|hot|sports|mv|xiqu)-(\\w+)-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配在三级页面的专题套专题
  val regexSubjectB2  = ("home-(movie|zongyi|tv|comic|kids|jilu|hot|mv|xiqu)-(\\w+)-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|mv\\d+|xiqu\\d+)-"
    + "(actor|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|mv\\d+|xiqu\\d+)").r

  //匹配第三方跳转的专题
  val regexSubjectC = "(thirdparty_\\d{1})[\\w\\-]+-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r

  //匹配少儿毛推荐的专题
  val regexSubjectD = "home-kids_home-(\\w+)-(kids\\d+)".r
  //匹配少儿三级页面中的专题
  val regexSubjectE = "home-kids_home-(\\w+)-(\\w+)-(kids\\d+)".r

  //匹配历史收藏中的专题
  val regexSubjectF = "home-(history)-[\\w\\-]+-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配历史收藏中的专题套专题
  val regexSubjectF2 = "home-(history)-[\\w\\-]+-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)-(actor|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r

  //暂时不清楚是匹配哪种情况，暂且保留此匹配项
  val regexSubjectG = "home-(movie|zongyi|tv|comic|kids|jilu|hot)-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+)".r

  def getSubjectCodeAndPath(path:String) = {
    regexSubjectA2 findFirstMatchIn path match {
      case Some(a2) => (a2.group(4),a2.group(1))::(a2.group(3),a2.group(1))::Nil
      case None => regexSubjectA findFirstMatchIn path match {
        case Some(a) => (a.group(3),a.group(1))::Nil
        case None => regexSubjectB2 findFirstMatchIn path match {
          case Some(b2) => (b2.group(3),b2.group(2))::(b2.group(4),b2.group(2))::Nil
          case None => regexSubjectB findFirstMatchIn path match {
            case Some(b) => (b.group(3),b.group(2))::Nil
            case None => regexSubjectC findFirstMatchIn path match {
              case Some(c) => (c.group(2),c.group(1))::Nil
              case None => regexSubjectD findFirstMatchIn path match {
                case Some(d) => (d.group(2),d.group(1))::Nil
                case None => regexSubjectE findFirstMatchIn path match {
                  case Some(e) => (e.group(3),e.group(2))::Nil
                  case None => regexSubjectF2 findFirstMatchIn path match {
                    case Some(f2) => (f2.group(2),f2.group(1))::(f2.group(3),f2.group(1))::Nil
                    case None => regexSubjectF findFirstMatchIn path match {
                      case Some(f) => (f.group(2),f.group(1))::Nil
                      case None => regexSubjectG findFirstMatchIn path match {
                        case Some(g) => (g.group(2),g.group(1))::Nil
                        case None => Nil
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def getSubjectCodeAndPathWithId(path:String,userId:String) = {
    getSubjectCodeAndPath(path).map(x => (x,userId))
  }
}
