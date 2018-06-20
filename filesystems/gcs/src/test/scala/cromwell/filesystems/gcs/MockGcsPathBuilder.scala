package cromwell.filesystems.gcs

import com.google.api.gax.retrying.RetrySettings
import com.google.cloud.NoCredentials
import com.google.cloud.storage.contrib.nio.CloudStorageConfiguration
import cromwell.cloudsupport.gcp.gcs.GcsStorage
import cromwell.core.WorkflowOptions
import cromwell.core.path.RequesterPaysCachedPathBuilder

object MockGcsPathBuilder {
  private def makeStorageOptions(project: Option[String] = Option("cromwell-test")) = GcsStorage.gcsStorageOptions(NoCredentials.getInstance(), RetrySettings.newBuilder().build(), project) 
  private val storageOptions = makeStorageOptions()
  private val apiStorage = GcsStorage.gcsStorage("cromwell-test-app", storageOptions)

  lazy val instance = new GcsPathBuilder(apiStorage, CloudStorageConfiguration.DEFAULT, storageOptions, RequesterPaysCachedPathBuilder.noCache) {
    override def isRequesterPaysCall(bucket: String) = false
  }
  
  def withOptions(workflowOptions: WorkflowOptions) = {
    val customStorageOptions = makeStorageOptions(workflowOptions.get("google_project").toOption)
    new GcsPathBuilder(apiStorage, GcsStorage.DefaultCloudStorageConfiguration, customStorageOptions, RequesterPaysCachedPathBuilder.noCache) {
      override def isRequesterPaysCall(bucket: String) = false
    }
  }
}
