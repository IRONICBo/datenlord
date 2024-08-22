//! The storage trait.

use async_trait::async_trait;

use super::{OpenFlag, StorageResult};

/// The trait defines an interface for I/O operations.
#[async_trait]
pub trait Storage {
    /// Opens a file with the given inode number and flags, returning a file
    /// handle.
    fn open(&self, ino: u64, fh: u64, flag: OpenFlag);

    /// Reads data from a file specified by the inode number and file handle,
    /// starting at the given offset and reading up to `len` bytes.
    /// version is used to check if the file has been modified since the last read.
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        len: usize,
        version: u64,
    ) -> StorageResult<Vec<u8>>;

    /// Writes data to a file specified by the inode number and file handle,
    /// starting at the given offset.
    /// version is used to check if the file has been modified since the last write,
    /// current write implementaion might need to read the file from storage first,
    /// so this function need to sync with read function.
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        buf: &[u8],
        version: u64,
    ) -> StorageResult<()>;

    /// Truncates a file specified by the inode number to a new size,
    /// given the old size and the new size.
    /// version is used to check if the file has been modified since the last truncate.
    /// current truncate implementaion might need to read the file from storage first,
    /// so this function need to sync with read function.
    async fn truncate(
        &self,
        ino: u64,
        old_size: u64,
        new_size: u64,
        version: u64,
    ) -> StorageResult<()>;

    /// Flushes any pending writes to a file specified by the inode number and
    /// file handle.
    async fn flush(&self, ino: u64, fh: u64) -> StorageResult<()>;

    /// Removes a file from the storage.
    async fn remove(&self, ino: u64) -> StorageResult<()>;

    /// Closes a file specified by the file handle.
    async fn close(&self, fh: u64) -> StorageResult<()>;
}
