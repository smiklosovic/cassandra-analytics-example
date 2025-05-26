package org.apache.cassandra.spark.analytics.example

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.DataTypes.{BinaryType, LongType}
import org.apache.spark.sql.types.StructType

import java.util.UUID

object App extends SparkUtils {

  def main(args: Array[String]): Unit = {

    val sparkConf = initialize()

    implicit val spark: SparkSession = SparkSession.builder.appName("Analytics Demo App").config(sparkConf).getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val sql: SQLContext = spark.sqlContext

    val allConfig: JobConfiguration = JobConfiguration(
      // write
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "local_dc" -> "dc1",
        "bulk_writer_cl" -> "LOCAL_QUORUM",
        "splits" -> "4",
        "rows" -> "10000000",
        "data_transport" -> "DIRECT",
        "commit_threads_per_instance" -> "2",
        "sstable_data_size_in_mib" -> "20"
      ),
      // read
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "local_dc" -> "dc1",
        "snapshotName" -> s"${UUID.randomUUID().toString}",
        "createSnapshot" -> "true",
        "sizing" -> "default"
      ))

    implicit val config: JobConfiguration = allConfig

    logger.info("Spark Conf: " + sparkConf.toDebugString)

    execute[Int]((_, _, _) => {
      val writtenRows = write()
      val readRows = read()

      if (writtenRows.isDefined && readRows.isDefined) {
        if (!writtenRows.get.exceptAll(readRows.get).isEmpty) {
          throw new IllegalStateException("The content of the dataframes differs")
        }
      }
      0
    }, {
      -1
    })
  }

  private def write()(implicit jobConfig: JobConfiguration, sql: SQLContext, sc: SparkContext): Option[Dataset[Row]] = {
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

  private def read()(implicit jobConfig: JobConfiguration, sql: SQLContext): Option[Dataset[Row]] = {
    if (!jobConfig.shouldRead) {
      Option.empty
    } else {
      Option(getReader(sql, jobConfig).load())
    }
  }

  private def genDataset(sc: JavaSparkContext, records: Long, slices: Int): RDD[Row] = {
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
