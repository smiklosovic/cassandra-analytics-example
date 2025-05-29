package org.apache.cassandra.spark.analytics.example

import org.apache.spark.SparkContext
import org.apache.spark.sql._

import java.util.UUID

object App extends SparkUtils {

  def resolveConfig(dataTransport: DataTransport): JobConfiguration = {

    val directConfig: JobConfiguration = JobConfiguration(
      // write
      Map(
        "sidecar_contact_points" -> "172.19.0.5,172.19.0.8,172.19.0.9",
        "keyspace" -> "spark_test",
        "table" -> "test2",
        "local_dc" -> "dc1",
        "bulk_writer_cl" -> "LOCAL_QUORUM",
        "splits" -> "6",
        "rows" -> "10000000",
        "data_transport" -> DIRECT.toString,
        "commit_threads_per_instance" -> "2",
        "sstable_data_size_in_mib" -> "20"
      ),
      // read
      Map(
        "sidecar_contact_points" -> "172.19.0.5,172.19.0.8,172.19.0.9",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "local_dc" -> "dc1",
        "snapshotName" -> s"${UUID.randomUUID().toString}",
        "createSnapshot" -> "true",
        "sizing" -> "default"
      ))

    val s3CompatConfig: JobConfiguration = JobConfiguration(
      // write
      Map(
        "sidecar_contact_points" -> "172.19.0.5,172.19.0.8,179.19.0.9",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "local_dc" -> "dc1",
        "bulk_writer_cl" -> "LOCAL_QUORUM",
        "number_splits" -> "6",
        "rows" -> "10000000",
        "data_transport" -> S3_COMPAT.toString,
        "commit_threads_per_instance" -> "2",
        "sstable_data_size_in_mib" -> "20",
        "data_transport_extension_class" -> classOf[LocalStorageTransportExtension].getCanonicalName,
        "storage_client_endpoint_override" -> "http://s3-mock:9090",
        "storage_client_max_chunk_size_in_bytes" -> "5242880",
        "max_size_per_sstable_bundle_in_bytes_s3_transport" -> "10485760",
        "max_job_duration_minutes" -> "10",
        "job_id" -> UUID.randomUUID().toString
      ),
      // read
      directConfig.readOptions)

    dataTransport match {
      case DIRECT => directConfig
      case S3_COMPAT => s3CompatConfig
    }
  }

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder.appName("Analytics Demo App").config(initialize()).getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val sql: SQLContext = spark.sqlContext
    implicit val config: JobConfiguration = resolveConfig(DIRECT)

    logger.info("Spark Conf: " + spark.sparkContext.getConf.toDebugString)

    logger.info(execute[Long]((_, _, _) => write().get.count(), { -1 }).toString)
  }
}
