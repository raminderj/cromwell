package cromwell.filesystems.gcs

import akka.actor.ActorSystem
import com.google.api.gax.retrying.RetrySettings
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import common.validation.ErrorOr.ErrorOr
import common.validation.Validation._
import cromwell.cloudsupport.gcp.GoogleConfiguration
import cromwell.cloudsupport.gcp.auth.GoogleAuthMode
import cromwell.cloudsupport.gcp.gcs.GcsStorage
import cromwell.core.WorkflowOptions
import cromwell.core.path.PathBuilderFactory
import cromwell.core.path.cache.BucketCache.DefaultBucketInformation
import cromwell.filesystems.gcs.GcsPathBuilderFactory.DefaultRetrySettings
import org.threeten.bp.Duration

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final case class GcsPathBuilderFactory(globalConfig: Config, instanceConfig: Config)
  extends PathBuilderFactory {
  import net.ceedubs.ficus.Ficus._
  // Parse the configuration and create a GoogleConfiguration
  val googleConf: GoogleConfiguration = GoogleConfiguration(globalConfig)
  // Extract the specified authentication mode for engine gcs filesystem, if any
  val authModeAsString: String = instanceConfig.as[String]("auth")
  // Validate it against the google configuration
  val authModeValidation: ErrorOr[GoogleAuthMode] = googleConf.auth(authModeAsString)
  val applicationName = googleConf.applicationName

  val authMode = authModeValidation.unsafe(s"Failed to create authentication mode for $authModeAsString")

  val defaultProject = instanceConfig.as[Option[String]]("project")

  val requesterPaysCacheTTL = instanceConfig.as[Option[FiniteDuration]]("requester-pays-cache-ttl").getOrElse(30.minutes)
  
  /*
   * The requester pays cache can be shared across all path builders.
   * That is because whether or not a bucket is requester pays is independent from the credentials used.
   */
  val requesterPaysGuavaCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(requesterPaysCacheTTL.length, requesterPaysCacheTTL.unit)
    .build[String, DefaultBucketInformation]()

  def withOptions(options: WorkflowOptions)(implicit as: ActorSystem, ec: ExecutionContext) = {
    GcsPathBuilder.fromAuthMode(authMode, applicationName, DefaultRetrySettings, GcsStorage.DefaultCloudStorageConfiguration, options, defaultProject, requesterPaysGuavaCache)
  }
}

object GcsPathBuilderFactory {
  lazy val DefaultRetrySettings: RetrySettings = RetrySettings.newBuilder()
    .setTotalTimeout(Duration.ofSeconds(30))
    .setInitialRetryDelay(Duration.ofMillis(100))
    .setRetryDelayMultiplier(1.1)
    .setMaxRetryDelay(Duration.ofSeconds(1))
    .setJittered(true)
    .setInitialRpcTimeout(Duration.ofMillis(100))
    .setRpcTimeoutMultiplier(1.1)
    .setMaxRpcTimeout(Duration.ofSeconds(5))
    .build()

  lazy val DefaultCloudStorageConfiguration = GcsStorage.DefaultCloudStorageConfiguration
}
