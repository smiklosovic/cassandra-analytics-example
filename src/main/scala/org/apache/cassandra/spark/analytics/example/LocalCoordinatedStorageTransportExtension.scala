package org.apache.cassandra.spark.analytics.example

import com.google.common.collect.ImmutableMap
import org.apache.cassandra.spark.transports.storage.extensions.{CoordinationSignalListener, StorageTransportConfiguration}
import org.apache.cassandra.spark.transports.storage.{StorageAccessConfiguration, StorageCredentialPair, StorageCredentials}
import org.apache.spark.SparkConf

class LocalCoordinatedStorageTransportExtension extends LocalStorageTransportExtension {
  private val BUCKET_NAME = "sbw-bucket"
  private var jobId: String = null
  private var coordinationSignalListener: CoordinationSignalListener = null

  override def initialize(jobId: String, conf: SparkConf, isOnDriver: Boolean): Unit = {
    this.jobId = jobId
  }

  override def getStorageConfiguration: StorageTransportConfiguration = {
    val additionalTags = ImmutableMap.of("additional-key", "additional-value")

    new StorageTransportConfiguration(
      "key-prefix",
      additionalTags,
      new StorageAccessConfiguration(
        "writeRegion",
        BUCKET_NAME,
        generateTokens.writeCredentials),
      ImmutableMap.of(
        "cluster_1",
        new StorageAccessConfiguration(
          "readRegion",
          BUCKET_NAME,
          generateTokens.readCredentials),
        "cluster_2",
        new StorageAccessConfiguration(
          "readRegion",
          BUCKET_NAME,
          generateTokens.readCredentials)))
  }

  override def setCoordinationSignalListener(listener: CoordinationSignalListener): Unit = {
    this.coordinationSignalListener = listener
  }

  override def onAllObjectsPersisted(objectsCount: Long, rowCount: Long, elapsedMillis: Long): Unit = {
    coordinationSignalListener.onStageReady(jobId)
  }

  override def onStageSucceeded(clusterId: String, elapsedMillis: Long): Unit = {
    coordinationSignalListener.onImportReady(jobId)
  }

  override def generateTokens = new StorageCredentialPair(
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
