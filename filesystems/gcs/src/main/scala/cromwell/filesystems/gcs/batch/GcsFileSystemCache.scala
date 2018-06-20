package cromwell.filesystems.gcs.batch

import com.google.cloud.storage.contrib.nio.{CloudStorageConfiguration, CloudStorageFileSystem}
import com.google.cloud.storage.{Storage, StorageOptions}
import com.google.common.cache.Cache
import cromwell.core.path.cache.FileSystemCache

class GcsFileSystemCache(cloudStorage: Storage,
                              cache: Cache[String, CloudStorageFileSystem],
                              cloudStorageConfiguration: CloudStorageConfiguration,
                              storageOptions: StorageOptions) extends FileSystemCache[CloudStorageFileSystem](cache) {
  override protected def retrieve(key: String) = {
    CloudStorageFileSystem.forBucket(key, cloudStorageConfiguration, storageOptions)
  }
}
