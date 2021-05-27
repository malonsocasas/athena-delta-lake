package com.backmarket.athena.connector.delta

import com.amazonaws.athena.connector.lambda.QueryStatusChecker
import com.amazonaws.athena.connector.lambda.data.{Block, BlockAllocator, BlockWriter, SchemaBuilder}
import com.amazonaws.athena.connector.lambda.domain.{Split, TableName}
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler
import com.amazonaws.athena.connector.lambda.metadata.{GetSplitsRequest, GetSplitsResponse, GetTableLayoutRequest, GetTableRequest, GetTableResponse, ListSchemasRequest, ListSchemasResponse, ListTablesRequest, ListTablesResponse}
import com.backmarket.athena.connector.delta.Config.S3_BUCKET
import io.delta.standalone.DeltaLog
import io.delta.standalone.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, StructField, TimestampType}
import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.hadoop.conf.Configuration
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import java.util
import java.util.logging.Logger
import scala.collection.JavaConverters._
import scala.util.matching.Regex

class DeltaMetadataHandler extends MetadataHandler("delta-lake") {
  val logger: Logger = Logger.getGlobal
  val s3: S3Client = S3Client.create()
  val S3_FOLDER_SUFFIX: String = "_$folder$"
  val HADOOP_CONF: Configuration = {
    val conf = new Configuration()
    conf.setLong("fs.s3a.multipart.size", 104857600)
    conf.setInt("fs.s3a.multipart.threshold", Integer.MAX_VALUE)
    conf.setBoolean("fs.s3a.impl.disable.cache", true)
    conf
  }

  val yearPattern: Regex = "(?<=year=)(\\d*)".r
  val monthPattern: Regex = "(?<=month=)(\\d*)".r
  val dayPattern: Regex = "(?<=day=)(\\d*)".r
  def getYear(path: String): Int = yearPattern.findAllIn(path).group(0).toInt
  def getMonth(path: String): Int = monthPattern.findAllIn(path).group(0).toInt
  def getDay(path: String): Int = dayPattern.findAllIn(path).group(0).toInt



  def listFolders(prefix: String = "") = {
    val listObjects: ListObjectsRequest = ListObjectsRequest
      .builder
      .prefix(prefix)
      .delimiter("/")
      .bucket(S3_BUCKET)
      .build
    s3.listObjects(listObjects)
      .contents.asScala
      .map(_.key())
      .filter(_.endsWith(S3_FOLDER_SUFFIX))
      .map(_.stripSuffix(S3_FOLDER_SUFFIX))
      .toSet
  }

  def addFieldToSchema(schemaBuilder: SchemaBuilder, field: StructField): SchemaBuilder = field.getDataType match {
    case data if data.isInstanceOf[StringType]    => schemaBuilder.addStringField(field.getName)
    case data if data.isInstanceOf[BooleanType]   => schemaBuilder.addBitField(field.getName)
    case data if data.isInstanceOf[ByteType]      => schemaBuilder.addTinyIntField(field.getName)
    case data if data.isInstanceOf[ShortType]     => schemaBuilder.addSmallIntField(field.getName)
    case data if data.isInstanceOf[IntegerType]   => schemaBuilder.addIntField(field.getName)
    case data if data.isInstanceOf[LongType]      => schemaBuilder.addBigIntField(field.getName)
    case data if data.isInstanceOf[FloatType]     => schemaBuilder.addFloat4Field(field.getName)
    case data if data.isInstanceOf[DoubleType]    => schemaBuilder.addFloat8Field(field.getName)
    case data if data.isInstanceOf[DateType]      => schemaBuilder.addDateDayField(field.getName)
    case data if data.isInstanceOf[TimestampType] => schemaBuilder.addDateMilliField(field.getName)
    case data if data.isInstanceOf[DecimalType]   => {
      val decimalType = data.asInstanceOf[DecimalType]
      schemaBuilder.addDecimalField(field.getName, decimalType.getPrecision, decimalType.getScale)
    }
  }

  override def doListSchemaNames(blockAllocator: BlockAllocator, listSchemasRequest: ListSchemasRequest): ListSchemasResponse = {
    logger.info(s"doListSchemaNames: ${listSchemasRequest.toString}")
    new ListSchemasResponse(listSchemasRequest.getCatalogName, listFolders().asJava)
  }

  override def doListTables(blockAllocator: BlockAllocator, listTablesRequest: ListTablesRequest): ListTablesResponse = {
    logger.info(s"doListTables: ${listTablesRequest.toString}")

    val schemaName = listTablesRequest.getSchemaName
    val prefix = schemaName + "/"
    val tables = listFolders(prefix)
      .map(_.stripPrefix(prefix))
      .map(new TableName(schemaName, _))
    new ListTablesResponse(schemaName, tables.asJava)
  }

  override def doGetTable(blockAllocator: BlockAllocator, getTableRequest: GetTableRequest): GetTableResponse = {
    val catalogName = getTableRequest.getCatalogName
    val tableName = getTableRequest.getTableName.getTableName
    val schemaName = getTableRequest.getTableName.getSchemaName

    val tablePath = s"s3a://$S3_BUCKET/$schemaName/$tableName/"
    logger.info(s"doGetTable: ${getTableRequest.toString} - tablePath: $tablePath")


    val log = DeltaLog.forTable(HADOOP_CONF, tablePath).snapshot
    val schema = log.getMetadata.getSchema.getFields
      .foldLeft(SchemaBuilder.newBuilder)(addFieldToSchema)
      .build()

    val partitions = log.getMetadata.getPartitionColumns

    new GetTableResponse(catalogName, getTableRequest.getTableName, schema, partitions.asScala.toSet.asJava)
  }

  override def getPartitions(blockWriter: BlockWriter, getTableLayoutRequest: GetTableLayoutRequest, queryStatusChecker: QueryStatusChecker): Unit = {
    logger.info(s"getPartitions: getTableLayoutRequest: ${getTableLayoutRequest.toString}")


    val tableName = getTableLayoutRequest.getTableName.getTableName
    val schemaName = getTableLayoutRequest.getTableName.getSchemaName

    val tablePath = s"s3a://$S3_BUCKET/$schemaName/$tableName/"
    val log = DeltaLog.forTable(HADOOP_CONF, tablePath).snapshot

    log.getAllFiles.asScala
      .map(file => {
        val path = file.getPath
        (getYear(path), getMonth(path), getDay(path))
      }).toSet[(Int, Int, Int)]
      .foreach{ case (yearVal, monthVal, dayVal) => {
        blockWriter.writeRows((block: Block, row: Int) => {
          def foo(block: Block, row: Int) = {
            var matched = true
            matched &= block.setValue("year", row, yearVal)
            matched &= block.setValue("month", row, monthVal)
            matched &= block.setValue("day", row, dayVal)
            //If all fields matches then we wrote 1 row during this call so we return 1
            if (matched) 1
            else 0
          }

          foo(block, row)
        })
      }}
  }

  override def doGetSplits(blockAllocator: BlockAllocator, getSplitsRequest: GetSplitsRequest): GetSplitsResponse = {
    logger.info(s"doGetSplits: ${getSplitsRequest.toString}")
    val catalogName: String = getSplitsRequest.getCatalogName
    val splits: util.Set[Split] = new util.HashSet[Split]

    val tableName = getSplitsRequest.getTableName.getTableName
    val schemaName = getSplitsRequest.getTableName.getSchemaName
    val tablePath = s"s3a://$S3_BUCKET/$schemaName/$tableName/"

    val log = DeltaLog.forTable(HADOOP_CONF, tablePath).snapshot
    val allFiles = log.getAllFiles
    val partitions: Block = getSplitsRequest.getPartitions

    val day: FieldReader = partitions.getFieldReader("day")
    val month: FieldReader = partitions.getFieldReader("month")
    val year: FieldReader = partitions.getFieldReader("year")


    (0 until partitions.getRowCount).foreach(i => {
      year.setPosition(i)
      month.setPosition(i)
      day.setPosition(i)

      val readYear = year.readInteger()
      val readMonth = month.readInteger()
      val readDay = day.readInteger()

      allFiles.asScala
        .filter(f => {
          val path = f.getPath
          (getYear(path), getMonth(path), getDay(path)) == (readYear, readMonth, readDay)
        })
        .foreach(f => {
          val split: Split = Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey())
            .add("year", String.valueOf(readYear))
            .add("month", String.valueOf(readMonth))
            .add("day", String.valueOf(readDay))
            .add("file", f.getPath)
            .build()
          splits.add(split)
        })
    })

    new GetSplitsResponse(catalogName, splits)
  }
}
