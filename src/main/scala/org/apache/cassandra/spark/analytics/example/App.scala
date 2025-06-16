package org.apache.cassandra.spark.analytics.example

import com.instaclustr.cassandra.Transformer.runTransformation
import com.instaclustr.cassandra.{CassandraPartitionsResolver, TransformerOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import java.util.UUID
import scala.util.Try

object App extends SparkUtils {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("Analytics Demo App").config(initialize()).getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val sql: SQLContext = spark.sqlContext
    logger.info("Spark Conf: " + sc.getConf.toDebugString)

    //executeJob(oneClusterWriteReadSameTable)
    //executeJob(oneClusterCopyTable())
    //executeJob(twoClustersCopyTable())
    //executeJob(twoClustersCoordinatedWrite())
    executeJob(sstableToParquet)
  }

  def executeJob[T](r: => T)(implicit spark: SparkSession): Unit = {
    Try.apply(r)
    spark.close()
  }

  /**
   * 1. write data to spark_test.test table
   * 2. transform all written data to Parquet files
   */
  private def sstableToParquet()(implicit spark: SparkSession, sc: SparkContext, sql: SQLContext): Unit = {
    writeOneCluster(JobConfiguration(
      // write
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "local_dc" -> "dc1",
        "bulk_writer_cl" -> "LOCAL_QUORUM",
        "rows" -> "10000000",
        "data_transport" -> DIRECT.toString,
      ),
      // read
      Map.empty))

    implicit val transformationJobConfig: JobConfiguration = JobConfiguration(
      Map.empty,
      Map.empty
    )

    logger.info(execute[Long]((_, _, _) => {
      val partitions = CassandraPartitionsResolver.partitions("dc1", "spark_test", "spark-master-1", 9043).toSeq

      val builder = new TransformerOptions.Builder()
        .keyspace("spark_test")
        .table("test")
        .output("/submit/output")
        .sidecar("spark-master-1:9043")
        .sidecar("cassandra-node-1:9043")
        .sidecar("cassandra-node-2:9043")

      sc.parallelize(partitions, 6).map(p => runTransformation(builder.partition(p).build())).count()
    }, { -1 }).toString)
  }

  /**
   * 1. write data to spark_test.test
   * 2. read data from spark_test.test
   */
  private def oneClusterWriteReadSameTable()(implicit spark: SparkSession, sc: SparkContext, sql: SQLContext): Unit = {
    writeOneCluster(JobConfiguration(
      // write
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "local_dc" -> "dc1",
        "bulk_writer_cl" -> "ALL",
        "rows" -> "10000000",
        "data_transport" -> DIRECT.toString,
      ),
      // read
      Map.empty))

    readOneCluster(JobConfiguration(
      // write
      Map.empty,
      // read
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "dc" -> "dc1",
        "consistencyLevel" -> "ONE",
        "snapshotName" -> s"${UUID.randomUUID().toString}",
        "createSnapshot" -> "true",
      )))
  }

  /**
   * 1. write data to spark_test.test
   * 2. read data from spark_test.test
   * 3. write data to spark_test.test2
   */
  private def oneClusterCopyTable()(implicit spark: SparkSession, sc: SparkContext, sql: SQLContext): Unit = {

    writeOneCluster(JobConfiguration(
      // write
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "local_dc" -> "dc1",
        "bulk_writer_cl" -> "ALL",
        //"splits" -> "6",
        "rows" -> "10000000",
        "data_transport" -> DIRECT.toString,
      ), Map.empty))

    copyTable(JobConfiguration(
      // write
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test2",
        "local_dc" -> "dc1",
        "bulk_writer_cl" -> "ALL",
        //"splits" -> "6",
        "rows" -> "10000000",
        "data_transport" -> DIRECT.toString,
      ),
      // read
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "dc" -> "dc1",
        "consistencyLevel" -> "ONE",
        "snapshotName" -> s"${UUID.randomUUID().toString}",
        "createSnapshot" -> "true",
        "sizing" -> "default"
      )))
  }

  /**
   * 1. write data to spark_test.test on cluster 1
   * 2. read data from spark_test.test on cluster 1
   * 3. write data to spark_test.test on cluster 2
   */
  private def twoClustersCopyTable()(implicit spark: SparkSession, sc: SparkContext, sql: SQLContext): Unit = {
    writeOneCluster(JobConfiguration(
      // write
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "local_dc" -> "dc1",
        "bulk_writer_cl" -> "ALL",
        "rows" -> "10000000",
        "data_transport" -> DIRECT.toString,
      ),
      // read
      Map.empty))

    copyTable(JobConfiguration(
      // write
      Map(
        "sidecar_contact_points" -> "spark-master-2,cassandra-node-3,cassandra-node-4",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "local_dc" -> "dc2",
        "bulk_writer_cl" -> "ALL",
        //"splits" -> "6",
        "rows" -> "10000000",
        "data_transport" -> DIRECT.toString,
      ),
      // read
      Map(
        "sidecar_contact_points" -> "spark-master-1,cassandra-node-1,cassandra-node-2",
        "keyspace" -> "spark_test",
        "table" -> "test",
        "dc" -> "dc1",
        "consistencyLevel" -> "ONE",
        "snapshotName" -> s"${UUID.randomUUID().toString}",
        "createSnapshot" -> "true",
        "sizing" -> "default"
      )))
  }

  private def twoClustersCoordinatedWrite()(implicit spark: SparkSession, sc: SparkContext, sql: SQLContext): Unit = {
    writeOneCluster(JobConfiguration(
      // write
      Map(
        "coordinated_write_config" ->
          """
            |{
            |	"cluster_1": {
            |		"sidecarContactPoints": ["spark-master-1:9043","cassandra-node-1:9043","cassandra-node-2:9043"],
            |		"localDc": "dc1"
            |	},
            |	"cluster_2": {
            |		"sidecarContactPoints": ["spark-master-2:9043","cassandra-node-3:9043","cassandra-node-4:9043"],
            |		"localDc": "dc2"
            |	}
            |}
            |""".stripMargin,
        "keyspace" -> "spark_test",
        "table" -> "test",
        "bulk_writer_cl" -> "LOCAL_QUORUM",
        "rows" -> "1000000",
        "data_transport" -> S3_COMPAT.toString,
        "data_transport_extension_class" -> classOf[LocalCoordinatedStorageTransportExtension].getCanonicalName,
        "storage_client_endpoint_override" -> "http://s3-mock:9090",
      ),
      // read
      Map.empty))
  }

  private def copyTable(configuration: JobConfiguration)(implicit spark: SparkSession, sc: SparkContext, sql: SQLContext): Unit = {
    implicit val conf: JobConfiguration = configuration
    logger.info("Copied rows from one table to another: " + execute[Long]((_, _, _) => writeExisting(read()).get.count(), { -1 }).toString)
  }

  private def writeOneCluster(configuration: JobConfiguration)(implicit spark: SparkSession, sc: SparkContext, sql: SQLContext): Unit = {
    implicit val conf: JobConfiguration = configuration
    logger.info("Written rows: " + execute[Long]((_, _, _) => write().get.count(), { -1 }).toString)
  }

  private def readOneCluster(configuration: JobConfiguration)(implicit spark: SparkSession, sc: SparkContext, sql: SQLContext): Unit = {
    implicit val conf: JobConfiguration = configuration
    logger.info("Read rows: " + execute[Long]((_, _, _) => read().get.count(), { -1 }).toString)
  }
}
