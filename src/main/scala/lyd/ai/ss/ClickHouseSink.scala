package lyd.ai.ss

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, Encoders}
import lyd.ai.tools._
import lyd.ai.ClickHouseUtils

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class ClickHouseSink[T <: Product: ClassTag](dbName: String, tableName: String, eventDataColumn: String)
                                            (getConnectionString: () => (String, Int)) // -> (host, port)
                                            (partitionFunc: (org.apache.spark.sql.Row) => java.sql.Date)
                                            (maxRetry: Int, ignoreThrowable: (Throwable) => Boolean)
                                            (implicit tag: TypeTag[T]) extends Sink with Serializable with Logging {

  override def addBatch(batchId: Long, data: DataFrame) = {

    val res = data.queryExecution.toRdd.mapPartitions{ iter =>

      val stateUpdateEncoder = Encoders.product[T]
      val schema = stateUpdateEncoder.schema
      val exprEncoder = stateUpdateEncoder.asInstanceOf[ExpressionEncoder[T]]

      if(iter.nonEmpty){

        Retry.retrySync(maxRetry, ignoreThrowable = ignoreThrowable){

          val clickHouseHostPort = getConnectionString()
          Utils.using(ClickHouseUtils.createConnection(clickHouseHostPort)){ connection =>

            val insertStatement = ClickHouseUtils.prepareInsertStatement(connection, dbName, tableName, eventDataColumn)(schema)

            iter.foreach{ internalRow =>
              val caseClassInstance = exprEncoder.resolveAndBind(
                schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
              ).fromRow(internalRow)
              val row = org.apache.spark.sql.Row.fromTuple(caseClassInstance)
              ClickHouseUtils.batchAdd(schema, row)(insertStatement)(partitionFunc)
            }

            val inserted = insertStatement.executeBatch().sum
            log.info(s"Inserted $inserted -> (${clickHouseHostPort._1}:${clickHouseHostPort._2})")

            List(inserted).toIterator

          } // end: close connection

        } // end: retry

      } else {
        Iterator.empty
      }

    } // end: mapPartition

    val insertedCount = res.collect().sum
    log.info(s"Batch $batchId's inserted total: $insertedCount")
  }
}