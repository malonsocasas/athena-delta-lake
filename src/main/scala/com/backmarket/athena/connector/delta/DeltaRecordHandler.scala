package com.backmarket.athena.connector.delta

import com.amazonaws.athena.connector.lambda.QueryStatusChecker
import com.amazonaws.athena.connector.lambda.data.{Block, BlockSpiller}
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter
import com.amazonaws.athena.connector.lambda.data.writers.extractors.{BitExtractor, DateMilliExtractor, DecimalExtractor, Extractor, Float4Extractor, Float8Extractor, IntExtractor, TinyIntExtractor, VarCharExtractor}
import com.amazonaws.athena.connector.lambda.data.writers.holders.{NullableDecimalHolder, NullableVarCharHolder}
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest
import com.backmarket.athena.connector.delta.Config.S3_BUCKET
import com.github.mjakubowski84.parquet4s.{Decimals, NullValue, ParquetReader, PrimitiveValue, RowParquetRecord}
import com.google.common.primitives.Longs
import org.apache.arrow.vector.holders.{NullableBitHolder, NullableDateMilliHolder, NullableFloat4Holder, NullableFloat8Holder, NullableIntHolder, NullableTinyIntHolder}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.parquet.io.api.Binary

import scala.collection.JavaConverters._
import java.util.logging.Logger

class DeltaRecordHandler extends RecordHandler("delta-lake") {

  val logger: Logger = Logger.getGlobal

  def getValue[T](context: Any, field: Field): Option[T] = {
    val record = context.asInstanceOf[RowParquetRecord]
    record.get(field.getName) match {
      case NullValue => None
      case value @ _ => Some(value.asInstanceOf[PrimitiveValue[T]].value)
    }
  }

  def getValueAsString(context: Any, field: Field): Option[String] = {
    val record = context.asInstanceOf[RowParquetRecord]
    record.get(field.getName) match {
      case NullValue => None
      case value @ _ => Some(value.asInstanceOf[PrimitiveValue[_]].value.toString)
    }
  }

  def getExtractor(field: Field): Extractor = field.getType.getTypeID match {
    case ArrowTypeID.Int if field.getType.asInstanceOf[ArrowType.Int].getBitWidth == 32 => new IntExtractor{
      override def extract(context: Any, value: NullableIntHolder): Unit = {
        val parquetValue = getValue[Int](context, field)
        value.isSet = if (parquetValue.isDefined) 1 else 0
        value.value = parquetValue.getOrElse(0)
      }
    }
    case ArrowTypeID.Int if field.getType.asInstanceOf[ArrowType.Int].getBitWidth == 8 => new TinyIntExtractor {
      override def extract(context: Any, value: NullableTinyIntHolder): Unit = {
        val parquetValue = getValue[Int](context, field)
        value.isSet = if (parquetValue.isDefined) 1 else 0
        value.value = parquetValue.map(_.toByte).getOrElse(0)
      }
    }
    case ArrowTypeID.Bool => new BitExtractor{
      override def extract(context: Any, value: NullableBitHolder): Unit = {
        val parquetValue = getValue[Boolean](context, field)
        value.isSet = if (parquetValue.isDefined) 1 else 0
        value.value = parquetValue.map(v => if (v) 1 else 0).getOrElse(0)
      }
    }
    case ArrowTypeID.Utf8 => new VarCharExtractor {
      override def extract(context: Any, value: NullableVarCharHolder): Unit = {
        val parquetValue = getValue[Binary](context, field)
        value.isSet = if (parquetValue.isDefined) 1 else 0
        value.value = parquetValue.map(_.toStringUsingUTF8).getOrElse("")
      }
    }
    case ArrowTypeID.Date => new DateMilliExtractor {
      override def extract(context: Any, value: NullableDateMilliHolder): Unit = {
        val parquetValue = getValue[Binary](context, field)
        value.isSet = if (parquetValue.isDefined) 1 else 0
        value.value = parquetValue.map(v => Longs.fromByteArray(v.getBytes)).getOrElse(0L)
      }
    }
    case ArrowTypeID.FloatingPoint if field.getType.asInstanceOf[ArrowType.FloatingPoint].getPrecision == FloatingPointPrecision.SINGLE => new Float4Extractor {
      override def extract(context: Any, value: NullableFloat4Holder): Unit = {
        val parquetValue = getValue[Float](context, field)
        value.isSet = if (parquetValue.isDefined) 1 else 0
        value.value = parquetValue.getOrElse(0f)
      }
    }
    case ArrowTypeID.FloatingPoint if field.getType.asInstanceOf[ArrowType.FloatingPoint].getPrecision == FloatingPointPrecision.DOUBLE => new Float8Extractor {
      override def extract(context: Any, value: NullableFloat8Holder): Unit = {
        val parquetValue = getValue[Double](context, field)
        value.isSet = if (parquetValue.isDefined) 1 else 0
        value.value = parquetValue.getOrElse(0d)
      }
    }
    case ArrowTypeID.Decimal => new DecimalExtractor {
      override def extract(context: Any, value: NullableDecimalHolder): Unit = {
        val parquetValue = getValue[Binary](context, field)
        value.isSet = if (parquetValue.isDefined) 1 else 0
        value.value = parquetValue.map(v => Decimals.decimalFromBinary(v).bigDecimal).getOrElse(BigDecimal(0).bigDecimal)
      }
    }
    case ArrowTypeID.Null => new Extractor {}
    case _ => new VarCharExtractor {
      override def extract(context: Any, value: NullableVarCharHolder): Unit = {
        val parquetValue = getValueAsString(context, field)
        value.isSet = if (parquetValue.isDefined) 1 else 0
        value.value = getValueAsString(context, field).getOrElse("")
      }
    }
  }

  def getLiteralExtractor(intValue: Int) = new IntExtractor{
    override def extract(context: Any, value: NullableIntHolder): Unit = {
      value.isSet = 1
      value.value = intValue
    }
  }

  override def readWithConstraint(blockSpiller: BlockSpiller, readRecordsRequest: ReadRecordsRequest, queryStatusChecker: QueryStatusChecker): Unit = {

    logger.info("readWithConstraint: enter - " + readRecordsRequest)

    val split = readRecordsRequest.getSplit

    val splitYear = split.getPropertyAsInt("year")
    val splitMonth = split.getPropertyAsInt("month")
    val splitDay = split.getPropertyAsInt("day")
    val splitFile = split.getProperty("file")

    val tableName = readRecordsRequest.getTableName.getTableName
    val schemaName = readRecordsRequest.getTableName.getSchemaName

    val tablePath = s"s3a://$S3_BUCKET/$schemaName/$tableName/"
    val filePath = s"$tablePath$splitFile"

    logger.info(s"readWithConstraint: split properties: year: $splitYear, month: $splitMonth, day: $splitDay, file: $splitFile, filePath: $filePath")

    val builder = readRecordsRequest.getSchema.getFields.asScala
      .foldLeft(GeneratedRowWriter.newBuilder(readRecordsRequest.getConstraints))((builder, field) => {
        if (field.getName == "year") builder.withExtractor(field.getName, getLiteralExtractor(splitYear))
        else if (field.getName == "month") builder.withExtractor(field.getName, getLiteralExtractor(splitMonth))
        else if (field.getName == "day") builder.withExtractor(field.getName, getLiteralExtractor(splitDay))
        else {
          val extractor = getExtractor(field)
          builder.withExtractor(field.getName, extractor)
        }
      })

    val rowWriter = builder.build
    val records = ParquetReader.read[RowParquetRecord](filePath)
    var countRecord = 0L
    records.foreach(record => {
      blockSpiller.writeRows((block: Block, rowNum: Int) => if (rowWriter.writeRow(block, rowNum, record)) 1 else 0)
      countRecord += 1
    })

    logger.info(s"Split finished with $countRecord records")
  }
}
