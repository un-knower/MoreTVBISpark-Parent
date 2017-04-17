package com.moretv.bi.util


import java.util.Collections
import scala.collection.JavaConversions._
import com.moretv.bi.util.baseclasee.{BaseClass, ModuleClass}
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}

/**
  * Created by witnes on 4/10/17.
  */

/**
  * Group By userId, sid
  * Metric is Ordered Timestamp
  */
class TimeAggregator extends UserDefinedAggregateFunction {


  val Threshold = 1

  override def inputSchema: StructType = StructType(
    StructField("timestamp", LongType)
      :: Nil
  )

  override def bufferSchema: StructType = StructType(
    StructField("timestamps", ArrayType(LongType))
      :: Nil
  )

  override def dataType: DataType = ArrayType(LongType)


  override def update(buffer: MutableAggregationBuffer, input: Row) = {


    val buf = buffer.getList[Long](0)

    buf.add(input.getAs[Long](0))

    buffer.update(0, buf.toList.sortWith(_ <= _))

  }


  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {

    val buf = buffer1.getList[Long](0)

    buf.addAll(buffer2.getList[Long](0))

    buffer1.update(0, buf.toList.sortWith(_ <= _))
  }

  override def initialize(buffer: MutableAggregationBuffer) = {

    buffer.update(0, DataTypes.createArrayType(LongType))

  }

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row) = {


    val buf1 = buffer.getList[LongType](0).toList

    val buf2 = buf1.tail

    val res =(buf1 zip buf2).map {
      case (p, n) => {
        (n.asInstanceOf[Long] - p.asInstanceOf[Long]).asInstanceOf[LongType]
      }
    }


  }

}
