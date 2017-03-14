package com.moretv.bi.report.medusa.util

/**
 * Created by xiajun on 2016/8/1.
 * 该util用于处理pathSpecial中的subject-专题名称-专题code与subject-专题名称的情况
  *
  * 残言化为句点，灯火黯然泯灭，忽见恍若隔世的你 不负沉默不言的抱歉 不见往日共度雨缠绵
 */
object MedusaSubjectNameCodeUtil {
  private val regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r
  private val regexSubjectName="""subject-([a-zA-Z0-9-\u4e00-\u9fa5]+)""".r
  // 获取 专题code
  def getSubjectCode(subject:String) = {
    regex findFirstMatchIn subject match {
      // 如果匹配成功，说明subject包含了专题code，直接返回专题code
      case Some(m) => {
        m.group(1)+m.group(2)
      }
      // 没有匹配成功，说明subject为专题名称，不包含专题code，因此直接返回专题名称
      case None => " "
    }
  }

  /*例子：假设pathSpecial为subject-儿歌一周热播榜,解析出 儿歌一周热播榜 */
  def getSubjectNameETL(subject:String) :String= {
    regexSubjectName findFirstMatchIn subject match {
      // 如果匹配成功，说明subject包含了专题名称，直接返回专题名称
      case Some(m) => {
        m.group(1)
      }
      case None => null
    }
  }

  def main(args: Array[String]) {
    val pathSpecial="subject-儿歌一周热播榜"
    println(getSubjectNameETL(pathSpecial))
  }


}
