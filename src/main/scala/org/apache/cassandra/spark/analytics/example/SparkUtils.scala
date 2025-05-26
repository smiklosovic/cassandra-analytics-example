package org.apache.cassandra.spark.analytics.example

import org.apache.cassandra.spark.KryoRegister
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import scala.util.Try

trait SparkUtils {
  val logger = LoggerFactory.getLogger(classOf[SparkUtils])

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
    spark.close()
    result.getOrElse(onFailure)
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
}

class JobConfiguration(val writeOptions: Map[String, String], val readOptions: Map[String, String]) {
  val rowCount: Int = writeOptions.get("rows").map(_.toInt).getOrElse(10000)

  def shouldWrite: Boolean = writeOptions.nonEmpty

  def shouldRead: Boolean = readOptions.nonEmpty

  def splits(defaultParalelism: Int): Int = writeOptions.get("splits").map(_.toInt).getOrElse(defaultParalelism)

  def onlyRead(): JobConfiguration = new JobConfiguration(Map(), readOptions)

  def onlyWrite(): JobConfiguration = new JobConfiguration(writeOptions, Map())
}

object JobConfiguration {
  def apply(writeOptions: Map[String, String], readOptions: Map[String, String]): JobConfiguration = new JobConfiguration(writeOptions, readOptions)
}
