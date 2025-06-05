package org.apache.cassandra.spark.analytics.example

import com.google.common.collect.ImmutableMap
import org.apache.cassandra.spark.transports.storage.extensions._
import org.apache.cassandra.spark.transports.storage.{StorageCredentialPair, StorageCredentials}
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import java.lang

class LocalStorageTransportExtension extends StorageTransportExtension {

  private val LOGGER = LoggerFactory.getLogger(classOf[LocalStorageTransportExtension])
  private val BUCKET_NAME = "sbw-bucket"
  private var jobId: String = null

  override def onObjectPersisted(bucket: String, key: String, sizeInBytes: Long): Unit = {
    val args: Array[Object] = Array(bucket, key, jobId, lang.Long.valueOf(sizeInBytes))
    LOGGER.info(">>>> Object {}/{} for job {} persisted with size {} bytes", args: _*)
  }

  override def onTransportStart(elapsedMillis: Long): Unit = {
    LOGGER.info(">>>> Transport started")
  }

  override def setCredentialChangeListener(credentialChangeListener: CredentialChangeListener): Unit = {}

  override def setObjectFailureListener(objectFailureListener: ObjectFailureListener): Unit = {}

  override def onAllObjectsPersisted(objectsCount: Long, rowCount: Long, elapsedMillis: Long): Unit = {
    val args: Array[Object] = Array(lang.Long.valueOf(objectsCount), lang.Long.valueOf(rowCount), lang.Long.valueOf(elapsedMillis))
    LOGGER.info(">>>> All {} objects, totaling {} rows, are persisted with elapsed time {}ms", args: _*)
  }

  override def onObjectApplied(bucket: String, key: String, sizeInBytes: Long, elapsedMillis: Long): Unit = {
    val args: Array[Object] = Array(bucket, key, lang.Long.valueOf(sizeInBytes), lang.Long.valueOf(elapsedMillis))
    LOGGER.info(">>>> Object {}/{} of size {} applied, elapsed ms: {}", args: _*)
  }

  override def onJobSucceeded(elapsedMillis: Long): Unit = {
    LOGGER.info(">>>> Job {} succeeded with elapsed time {}ms", jobId, elapsedMillis)
  }

  override def onJobFailed(elapsedMillis: Long, throwable: Throwable): Unit = {
    val args: Array[Object] = Array(jobId, lang.Long.valueOf(elapsedMillis), throwable);
    LOGGER.error(">>>> Job {} failed after {}ms", args: _*)
  }

  override def onStageSucceeded(clusterId: String, elapsedMillis: Long): Unit = {
    val args: Array[Object] = Array(jobId, clusterId, lang.Long.valueOf(elapsedMillis))
    LOGGER.info(">>>> Job {} has all objects staged at cluster {} after {}ms", args: _*)
  }

  override def onStageFailed(clusterId: String, cause: Throwable): Unit = {
    LOGGER.error(">>>> Job {} failed to stage objects at cluster {}", jobId, clusterId, cause)
  }

  override def onImportSucceeded(clusterId: String, elapsedMillis: Long): Unit = {
    val args: Array[Object] = Array(jobId, clusterId, lang.Long.valueOf(elapsedMillis))
    LOGGER.info(">>>> Job {} has all objects imported at cluster {} after {}ms", args: _*)
  }

  override def onImportFailed(clusterId: String, cause: Throwable): Unit = {
    val args: Array[Object] = Array(clusterId, cause)
    LOGGER.error(">>>> Cluster {} failed to apply objects", args: _*)
  }

  override def setCoordinationSignalListener(listener: CoordinationSignalListener): Unit = {
    LOGGER.info(">>>> CoordinationSignalListener initialized. listener={}", listener.toString)
  }

  override def initialize(jobId: String, conf: SparkConf, isOnDriver: Boolean): Unit = {
    this.jobId = jobId;
    LOGGER.info(">>>> Extension was initialized")
  }



  override def getStorageConfiguration: StorageTransportConfiguration = {
    val additionalTags = ImmutableMap.of("additional-key", "additional-value")
    new StorageTransportConfiguration(
      BUCKET_NAME, "us-west-1",
      BUCKET_NAME, "eu-west-1",
      "test-cluster",
      generateTokens,
      additionalTags)
  }

  def generateTokens = new StorageCredentialPair(
    "writeRegion",
    new StorageCredentials(
      "writeKey",
      "writeSecret",
      "writeSessionToken"),
    "readRegion",
    new StorageCredentials(
      "readKey",
      "readSecret",
      "readSessionToken"))
}
