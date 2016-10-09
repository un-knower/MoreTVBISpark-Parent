package com.moretv.bi.report.medusa.util

/**
 * Created by xiajun on 2016/8/1.
 * 该util用于处理pathSpecial中的subject-专题名称-专题code与subject-专题名称的情况
 */
object MedusaSubjectNameCodeUtil {
  private val regex="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu)([0-9]+)""".r
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
}
