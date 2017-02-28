package com.moretv.bi.util

import org.apache.spark.sql.Row
import scala.reflect.runtime.universe._

/**
  * Created by 连凯 on 2016/4/23.
  */
object ImplicitClass {

  implicit class Row2Tuple(row:Row){

    def toArray = {
      (0 until row.size).map(i => {
        row.get(i)
      })
    }
    def toTuple1[T] = (row.getAs[T](0))
    def toTuple2[T1,T2] = (row.getAs[T1](0),row.getAs[T2](1))
    def toTuple3[T1,T2,T3] = (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2))
    def toTuple4[T1,T2,T3,T4] = (row.getAs[T1](0),row.getAs[T2](1),row.getAs[T3](2),row.getAs[T4](3))
  }
  //    def toTuple[T:TypeTag]:T = {
  //      val TypeRef(pre,typeName,params) = typeOf[T]
  //      val typeList = params.map(_.toString)
  //      typeList.size match {
  //        case 2 => (toActualType(0),toActualType(1))
  //      }
  //    }
  //
  //    def toActualType[A](i:Int) =
}
