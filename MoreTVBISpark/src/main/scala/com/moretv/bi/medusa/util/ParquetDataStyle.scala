package src.com.moretv.bi.medusa.util

/**
 * Created by Administrator on 2016/3/28.
 */
object ParquetDataStyle{
  case class DATE_PRODUCT_INFO(Mac:String,Date_code:String,Product_code:String) {}
  case class ALL_CRASH_INFO(fileName:String,Mac:String,App_version_name:String,App_version_code:String,Crash_key:String,
                            Stack_trace:String,Date_code:String,Product_code:String){}
  case class ALL_CRASH_INFO_INCLUDE_MD5(fileName:String,fileName_md5:String,Mac:String,App_version_name:String,
                                        App_version_name_md5:String, App_version_code:String,App_version_code_md5:String,
                                        android_version:String,Stack_trace:String,Stack_trace_md5:String,Date_code:String,
                                        Product_code:String){}
}

