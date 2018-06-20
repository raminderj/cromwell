package cromwell.filesystems.gcs

import java.io._
import java.net.URI
import java.nio.channels.Channels
import java.nio.charset.Charset
import java.nio.file.NoSuchFileException

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import better.files.File.OpenOptions
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.gax.retrying.RetrySettings
import com.google.auth.Credentials
import com.google.cloud.storage.Storage.{BlobSourceOption, BlobTargetOption, BucketField, BucketGetOption}
import com.google.cloud.storage.contrib.nio.{CloudStorageConfiguration, CloudStorageFileSystem, CloudStoragePath}
import com.google.cloud.storage.{BlobId, BlobInfo, StorageException, StorageOptions}
import com.google.common.cache.Cache
import com.google.common.net.UrlEscapers
import cromwell.cloudsupport.gcp.auth.GoogleAuthMode
import cromwell.cloudsupport.gcp.gcs.GcsStorage
import cromwell.core.WorkflowOptions
import cromwell.core.path.{NioPath, Path, PathBuilder, RequesterPaysCachedPathBuilder}
import cromwell.filesystems.gcs.GcsPathBuilder._
import cromwell.filesystems.gcs.GoogleUtil._
import cromwell.util.TryWithResource._
import mouse.all._

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Codec
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
object GcsPathBuilder {
  implicit class EnhancedCromwellPath(val path: Path) extends AnyVal {
    def requesterPaysProject: Option[String] = path match {
      case gcs: GcsPath => gcs.requesterPays.option(gcs.projectId)
      case _ => None
    }

    def requesterPaysGSUtilFlagList = requesterPaysProject.map(project => List("-u", project)).getOrElse(List.empty)
    def requesterPaysGSUtilFlag = requesterPaysGSUtilFlagList.mkString(" ")
  }

  /*
    * Provides some level of validation of GCS bucket names
    * This is meant to alert the user early if they mistyped a gcs path in their workflow / inputs and not to validate
    * exact bucket syntax, which is done by GCS.
    * See https://cloud.google.com/storage/docs/naming for full spec
  */
  val GcsBucketPattern =
    """
      (?x)                                      # Turn on comments and whitespace insensitivity
      ^gs://
      (                                         # Begin capturing group for gcs bucket name
        [a-z0-9][a-z0-9-_\\.]+[a-z0-9]          # Regex for bucket name - soft validation, see comment above
      )                                         # End capturing group for gcs bucket name
      (?:
        /.*                                     # No validation here
      )?
    """.trim.r

  sealed trait GcsPathValidation
  case class ValidFullGcsPath(bucket: String, path: String) extends GcsPathValidation
  case object PossiblyValidRelativeGcsPath extends GcsPathValidation
  sealed trait InvalidGcsPath extends GcsPathValidation {
    def pathString: String
    def errorMessage: String
  }
  final case class InvalidScheme(pathString: String) extends InvalidGcsPath {
    override def errorMessage = s"Cloud Storage URIs must have 'gs' scheme: $pathString"
  }
  final case class InvalidFullGcsPath(pathString: String) extends InvalidGcsPath {
    override def errorMessage = {
      s"""
         |The path '$pathString' does not seem to be a valid GCS path.
         |Please check that it starts with gs:// and that the bucket and object follow GCS naming guidelines at
         |https://cloud.google.com/storage/docs/naming.
      """.stripMargin.replace("\n", " ").trim
    }
  }
  final case class UnparseableGcsPath(pathString: String, throwable: Throwable) extends InvalidGcsPath {
    override def errorMessage: String =
      List(s"The specified GCS path '$pathString' does not parse as a URI.", throwable.getMessage).mkString("\n")
  }

  case class CromwellGcsFileSystem(filesystem: CloudStorageFileSystem, requesterPays: Boolean)

  /**
    * Tries to extract a bucket name out of the provided string using rules less strict that URI hostname,
    * as GCS allows albeit discourages.
    */
  private def softBucketParsing(string: String): Option[String] = string match {
    case GcsBucketPattern(bucket) => Option(bucket)
    case _ => None
  }

  def validateGcsPath(string: String): GcsPathValidation = {
    Try {
      val uri = URI.create(UrlEscapers.urlFragmentEscaper().escape(string))
      if (uri.getScheme == null) PossiblyValidRelativeGcsPath
      else if (uri.getScheme.equalsIgnoreCase(CloudStorageFileSystem.URI_SCHEME)) {
        if (uri.getHost == null) {
          softBucketParsing(string) map { ValidFullGcsPath(_, uri.getPath) } getOrElse InvalidFullGcsPath(string)
        } else ValidFullGcsPath(uri.getHost, uri.getPath)
      } else InvalidScheme(string)
    } recover { case t => UnparseableGcsPath(string, t) } get
  }

  def isGcsPath(nioPath: NioPath): Boolean = {
    nioPath.getFileSystem.provider().getScheme.equalsIgnoreCase(CloudStorageFileSystem.URI_SCHEME)
  }

  def fromAuthMode(authMode: GoogleAuthMode,
                   applicationName: String,
                   retrySettings: RetrySettings,
                   cloudStorageConfiguration: CloudStorageConfiguration,
                   options: WorkflowOptions,
                   defaultProject: Option[String],
                   requesterPaysCache: Cache[String, java.lang.Boolean])(implicit as: ActorSystem, ec: ExecutionContext): Future[GcsPathBuilder] = {
    authMode.retryCredential(options) map { credentials =>
      fromCredentials(credentials,
        applicationName,
        retrySettings,
        cloudStorageConfiguration,
        options,
        defaultProject,
        requesterPaysCache
      )
    }
  }

  def fromCredentials(credentials: Credentials,
                      applicationName: String,
                      retrySettings: RetrySettings,
                      cloudStorageConfiguration: CloudStorageConfiguration,
                      options: WorkflowOptions,
                      defaultProject: Option[String],
                      requesterPaysCache: Cache[String, java.lang.Boolean]): GcsPathBuilder = {
    // Grab the google project from Workflow Options if specified and set
    // that to be the project used by the StorageOptions Builder. If it's not
    // specified use the default project mentioned in config file
    val project: Option[String] =  options.get("google_project").toOption match {
      case Some(googleProject) => Option(googleProject)
      case None => defaultProject
    }

    val storageOptions = GcsStorage.gcsStorageOptions(credentials, retrySettings, project)

    // Create a com.google.api.services.storage.Storage
    // This is the underlying api used by com.google.cloud.storage
    // By bypassing com.google.cloud.storage, we can create low level requests that can be batched
    val apiStorage = GcsStorage.gcsStorage(applicationName, storageOptions)

    new GcsPathBuilder(apiStorage, cloudStorageConfiguration, storageOptions, requesterPaysCache)
  }
}

class GcsPathBuilder(apiStorage: com.google.api.services.storage.Storage,
                     cloudStorageConfiguration: CloudStorageConfiguration,
                     storageOptions: StorageOptions,
                     override val requesterPaysCache: Cache[String, java.lang.Boolean]) extends PathBuilder with RequesterPaysCachedPathBuilder[CloudStorageFileSystem] {
  private lazy val cloudStorage = storageOptions.getService

  // This could be cached too, per path builder
  def makeFileSystem(bucket: String) = {
    CloudStorageFileSystem.forBucket(bucket, cloudStorageConfiguration, storageOptions)
  }

  private[gcs] val projectId = storageOptions.getProjectId

  /**
    * Tries to create a new GcsPath from a String representing an absolute gcs path: gs://<bucket>[/<path>].
    *
    * Note that this creates a new CloudStorageFileSystemProvider for every Path created, hence making it unsuitable for
    * file copying using the nio copy method. Cromwell currently uses a lower level API which works around this problem.
    *
    * If you plan on using the nio copy method make sure to take this into consideration.
    *
    * Also see https://github.com/GoogleCloudPlatform/google-cloud-java/issues/1343
    */
  def build(string: String): Try[GcsPath] = {
    validateGcsPath(string) match {
      case ValidFullGcsPath(bucket, path) =>
        Try {
          val fileSystem = makeFileSystem(bucket)
          val cloudStoragePath = fileSystem.getPath(path)
          val requesterPays = isRequesterPaysCached(bucket)
          GcsPath(cloudStoragePath, apiStorage, cloudStorage, projectId, requesterPays)
        }
      case PossiblyValidRelativeGcsPath => Failure(new IllegalArgumentException(s"$string does not have a gcs scheme"))
      case invalid: InvalidGcsPath => Failure(new IllegalArgumentException(invalid.errorMessage))
    }
  }

  override def name: String = "Google Cloud Storage"

  // This is a synchronous call to GCS but would be too complex to truly asynchronify for now
  override protected def isRequesterPaysCall(bucket: String) = {
    /*
     * It turns out that making a request to determine if a bucket has requester pays, when the the bucket indeed has it enabled,
     * fails without passing a billing project..
     * I see 2 options:
     * 1) Always pass a billing project for that request (potentially occurring unnecessary cost, but allowing to get the real value from the object metadata)
     * 2) Try without billing project and parse the failure to see if it was due to the bucket having requester pays
     * 
     * Going with 2 for now.
     */
    Try(cloudStorage.get(bucket, BucketGetOption.fields(BucketField.ID)))
      // If it works, it means requester pays is not required since we did not specify a project ID. Use the ID field to limit the size of the response.
      .map(_ => false)
      .recover({
        case storageException: StorageException if storageException.getCode == 400 &&
          storageException.getMessage.contains("Bucket is requester pays bucket but no user project provided") => true
      })
      /*
       * If it fails for some other reason (for instance the path does not exist), return false.
       * We want to avoid throwing here because we're only trying to determine a value for requester pays when building
       * paths from this bucket, we can't want to make assumptions on what paths are going to be built from this bucket
       * and for what purpose.
       */
      .toOption.getOrElse(false)
  }
}

case class GcsPath private[gcs](nioPath: NioPath,
                                apiStorage: com.google.api.services.storage.Storage,
                                cloudStorage: com.google.cloud.storage.Storage,
                                projectId: String,
                                requesterPays: Boolean) extends Path {
  lazy val blob = BlobId.of(cloudStoragePath.bucket, cloudStoragePath.toRealPath().toString)

  // Will be null if requesterPays is false
  val requesterPaysProject = requesterPays.option(projectId).orNull

  override protected def newPath(nioPath: NioPath): GcsPath = GcsPath(nioPath, apiStorage, cloudStorage, projectId, requesterPays)

  override def pathAsString: String = {
    val host = cloudStoragePath.bucket().stripSuffix("/")
    val path = cloudStoragePath.toString.stripPrefix("/")
    s"${CloudStorageFileSystem.URI_SCHEME}://$host/$path"
  }

  override def writeContent(content: String)(openOptions: OpenOptions, codec: Codec) = {
    cloudStorage.create(
      BlobInfo.newBuilder(blob)
        .setContentType(ContentTypes.`text/plain(UTF-8)`.value)
        .build(),
      content.getBytes(codec.charSet),
      BlobTargetOption.userProject(requesterPaysProject)
    )
    this
  }

  /***
    * This method needs to be overridden to make it work with requester pays. We need to go around Nio
    * as currently it doesn't support to set the billing project id. Google Cloud Storage already has
    * code in place to set the billing project (inside HttpStorageRpc) but Nio does not pass it even though
    * it's available. In future when it is supported, remove this method and wire billing project into code
    * wherever necessary
    */
  override def mediaInputStream: InputStream = {
    Try{
      Channels.newInputStream(cloudStorage.reader(blob, BlobSourceOption.userProject(requesterPaysProject)))
    } match {
      case Success(inputStream) => inputStream
      case Failure(e: GoogleJsonResponseException) if e.getStatusCode == StatusCodes.NotFound.intValue =>
        throw new NoSuchFileException(pathAsString)
      case Failure(e) => e.getMessage
        throw new IOException(s"Failed to open an input stream for $pathAsString: ${e.getMessage}", e)
    }
  }

  /***
    * This method needs to be overridden to make it work with requester pays. We need to go around Nio
    * as currently it doesn't support to set the billing project id. Google Cloud Storage already has
    * code in place to set the billing project (inside HttpStorageRpc) but Nio does not pass it even though
    * it's available. In future when it is supported, remove this method and wire billing project into code
    * wherever necessary
    */
  override def readContentAsString(implicit codec: Codec): String = {
    withInputStream(readLinesAsString)
  }

  private def readLinesAsString(inputStream: InputStream)(implicit codec: Codec): String = {
    val byteArray = Stream.continually(inputStream.read).takeWhile(_ != -1).map(_.toByte).toArray
    new String(byteArray, Charset.forName(codec.name))
  }

  /***
    * This method needs to be overridden to make it work with requester pays. We need to go around Nio
    * as currently it doesn't support to set the billing project id. Google Cloud Storage already has
    * code in place to set the billing project (inside HttpStorageRpc) but Nio does not pass it even though
    * it's available. In future when it is supported, remove this method and wire billing project into code
    * wherever necessary
    */
  override def readAllLinesInFile(implicit codec: Codec): Traversable[String] = withInputStream { is =>
    val reader = new BufferedReader(new InputStreamReader(is, codec.name))
    Stream.continually(reader.readLine()).takeWhile(_ != null).toList
  }

  /*
   * The input stream will be closed when this method returns, which means the f function
   * cannot leak an open stream.
   */
  private def withInputStream[A](f: InputStream => A): A = {
    tryWithResource(() => mediaInputStream)(inputStream => f(inputStream)).recoverWith({
      case failure => Failure(new IOException(s"Failed to open an input stream for $pathAsString", failure))
    }).get
  }

  override def pathWithoutScheme: String = {
    val gcsPath = cloudStoragePath
    gcsPath.bucket + gcsPath.toAbsolutePath.toString
  }

  def cloudStoragePath: CloudStoragePath = nioPath match {
    case gcsPath: CloudStoragePath => gcsPath
    case _ => throw new RuntimeException(s"Internal path was not a cloud storage path: $nioPath")
  }
}
