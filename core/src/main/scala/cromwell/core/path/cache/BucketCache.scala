package cromwell.core.path.cache

import com.google.common.cache.{Cache, CacheBuilder}

object BucketCache {
  case class DefaultBucketInformation(requesterPays: Boolean)
  val noCache = CacheBuilder.newBuilder().maximumSize(0).build[String, DefaultBucketInformation]()
}

/**
  * Use to cache meta information about buckets. Keys are bucket names.
  */
abstract class BucketCache[A <: Object](cache: Cache[String, A]) extends CacheHelper(cache)
