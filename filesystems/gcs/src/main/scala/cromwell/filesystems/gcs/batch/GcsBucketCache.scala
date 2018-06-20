package cromwell.filesystems.gcs.batch

import com.google.cloud.storage.{Storage, StorageException}
import com.google.cloud.storage.Storage.{BucketField, BucketGetOption}
import com.google.common.cache.Cache
import cromwell.core.path.cache.BucketCache
import cromwell.core.path.cache.BucketCache.DefaultBucketInformation

import scala.util.Try

class GcsBucketCache(cloudStorage: Storage, cache: Cache[String, DefaultBucketInformation]) extends BucketCache[DefaultBucketInformation](cache) {
  override protected def retrieve(key: String) = {
    /*
     * It turns out that making a request to determine if a bucket has requester pays, when the the bucket indeed has it enabled,
     * fails without passing a billing project..
     * I see 2 options:
     * 1) Always pass a billing project for that request (potentially occurring unnecessary cost, but allowing to get the real value from the object metadata)
     * 2) Try without billing project and parse the failure to see if it was due to the bucket having requester pays
     * 
     * Going with 2 for now. Note that if
     */
    val requesterPaysRequired = Try(cloudStorage.get(key, BucketGetOption.fields(BucketField.ID)))
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

    DefaultBucketInformation(requesterPaysRequired)
  }
}
