package org.apache.cassandra.spark.analytics.example

import org.apache.cassandra.spark.KryoRegister
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.DataTypes.{BinaryType, LongType}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Try

trait SparkUtils extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(classOf[SparkUtils])

  def initialize(): SparkConf = {
    val sparkConf = new SparkConf()
    System.setProperty("SKIP_STARTUP_VALIDATIONS", "true")
    BulkSparkConf.setupSparkConf(sparkConf, true)

    if (sparkConf.get("SKIP_STARTUP_VALIDATIONS", "").isEmpty) {
      val old = sparkConf.get("spark.executor.extraJavaOptions")
      sparkConf.set("spark.executor.extraJavaOptions", "-DSKIP_STARTUP_VALIDATIONS=true " + old)
    }

    KryoRegister.setup(sparkConf)
    sparkConf
  }

  def execute[T](f: (JobConfiguration, SparkContext, SQLContext) => T,
                 onFailure: => T)(implicit conf: JobConfiguration,
                                  spark: SparkSession,
                                  sc: SparkContext,
                                  sql: SQLContext): T = {
    val result = Try(f.apply(conf, sc, sql))
    if (result.isFailure) {
      logger.info(s"${result.failed.get}")
      throw result.failed.get
    } else {
      result.getOrElse(onFailure)
    }
  }

  protected def convertStringToBytes(string: String): Array[Byte] = {
    val stringBytes = string.getBytes
    val buf = ByteBuffer.allocate(stringBytes.length)
    buf.put(stringBytes)
    buf.array
  }

  protected def getWriter(df: Dataset[Row], jobConfig: JobConfiguration): DataFrameWriter[Row] = {
    val writer: DataFrameWriter[Row] = df.write.format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
    writer.options(jobConfig.writeOptions)
    writer.mode("append")
  }

  protected def getReader(sql: SQLContext, jobConfig: JobConfiguration): DataFrameReader = {
    val reader = sql.read.format("org.apache.cassandra.spark.sparksql.CassandraDataSource")
    reader.options(jobConfig.readOptions)
  }

  def writeExisting(readRows: Option[Dataset[Row]])(implicit jobConfig: JobConfiguration, sql: SQLContext, sc: SparkContext): Option[Dataset[Row]] = {
    if (!jobConfig.shouldWrite || readRows.isEmpty) {
      Option.empty
    } else {
      getWriter(readRows.get, jobConfig).save()
      readRows
    }
  }

  def write()(implicit jobConfig: JobConfiguration, sql: SQLContext, sc: SparkContext): Option[Dataset[Row]] = {
    if (!jobConfig.shouldWrite)
      Option.empty
    else {
      val schema: StructType = new StructType()
        .add("id", LongType, nullable = false)
        .add("course", BinaryType, nullable = false)
        .add("marks", LongType, nullable = false)

      val slices: Int = jobConfig.splits(sc.defaultParallelism)
      val rows: RDD[Row] = genDataset(sc, jobConfig.rowCount, slices)
      val df: Dataset[Row] = sql.createDataFrame(rows, schema)

      getWriter(df, jobConfig).save()
      Option(df)
    }
  }

  def read()(implicit jobConfig: JobConfiguration, sql: SQLContext): Option[Dataset[Row]] = {
    if (!jobConfig.shouldRead) {
      Option.empty
    } else {
      Option(getReader(sql, jobConfig).load())
    }
  }

  def genDataset(sc: JavaSparkContext, records: Long, slices: Int): RDD[Row] = {
    val recordsPerPartition: Long = records / slices
    val remainder: Long = records - (recordsPerPartition * slices)
    val seq = 0 until slices

    sc.parallelize(seq, slices)
      .mapPartitionsWithIndex((index, _) => {
        val firstRecordNumber: Long = index * recordsPerPartition
        val recordsToGenerate: Int = if (index == slices - 1) (recordsPerPartition + remainder).toInt else recordsPerPartition.toInt
        (0 until recordsToGenerate).iterator.map(offset => {
          val i = firstRecordNumber + offset
          val courseName: Array[Byte] = convertStringToBytes(UUID.randomUUID().toString)
          RowFactory.create(Long.box(i), courseName, Long.box(i))
        })
      })
  }
}

class JobConfiguration(val writeOptions: Map[String, String], val readOptions: Map[String, String]) {
  val rowCount: Int = writeOptions.get("rows").map(_.toInt).getOrElse(10000)

  def shouldWrite: Boolean = writeOptions.nonEmpty

  def shouldRead: Boolean = readOptions.nonEmpty

  def splits(defaultParalelism: Int): Int = writeOptions.get("splits").map(_.toInt).getOrElse(defaultParalelism)

  def onlyRead(): JobConfiguration = new JobConfiguration(Map(), readOptions)

  def onlyWrite(): JobConfiguration = new JobConfiguration(writeOptions, Map())
}

sealed trait DataTransport
case object DIRECT extends DataTransport {
  override def toString: String = "DIRECT"
}
case object S3_COMPAT extends DataTransport {
  override def toString: String = "S3_COMPAT"
}

object JobConfiguration {
  def apply(writeOptions: Map[String, String], readOptions: Map[String, String]): JobConfiguration = new JobConfiguration(writeOptions, readOptions)
}
