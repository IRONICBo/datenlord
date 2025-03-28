//! The implementation of filesystem related utilities
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clippy_utilities::Cast;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::stat::{Mode, SFlag};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::async_fuse::fuse::protocol::FuseAttr;
use crate::common::error::{DatenLordError, DatenLordResult};
use crate::common::util;

/// Build error result from `nix` error code
/// # Errors
///
/// Return the built `Err(anyhow::Error(..))`
pub fn build_error_result_from_errno<T>(error_code: Errno, err_msg: String) -> DatenLordResult<T> {
    Err(DatenLordError::from(
        anyhow::Error::new(error_code).context(err_msg),
    ))
}

/// The node ID of the root inode
pub const ROOT_ID: u64 = 1;

/// The type of i-number
pub type INum = u64;

/// POSIX statvfs parameters
#[derive(Debug)]
pub struct StatFsParam {
    /// The number of blocks in the filesystem
    pub blocks: u64,
    /// The number of free blocks
    pub bfree: u64,
    /// The number of free blocks for non-privilege users
    pub bavail: u64,
    /// The number of inodes
    pub files: u64,
    /// The number of free inodes
    pub f_free: u64,
    /// Block size
    pub bsize: u32,
    /// Maximum file name length
    pub namelen: u32,
    /// Fragment size
    pub frsize: u32,
}

/// Set attribute parameters
#[derive(Debug)]
pub struct SetAttrParam {
    /// FUSE set attribute bit mask
    pub valid: u32,
    /// File handler
    pub fh: Option<u64>,
    /// File mode
    pub mode: Option<u32>,
    /// User ID
    pub u_id: Option<u32>,
    /// Group ID
    pub g_id: Option<u32>,
    /// File size
    pub size: Option<u64>,
    /// Lock owner
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: Option<u64>,
    /// Access time
    pub a_time: Option<SystemTime>,
    /// Content modified time
    pub m_time: Option<SystemTime>,
    /// Meta-data changed time seconds
    #[cfg(feature = "abi-7-23")]
    pub c_time: Option<SystemTime>,
}

/// Create parameters
#[derive(Debug)]
pub struct CreateParam {
    /// Parent directory i-number
    pub parent: INum,
    /// File name
    pub name: String,
    /// File mode
    pub mode: u32,
    /// File flags
    pub rdev: u32,
    /// User ID
    pub uid: u32,
    /// Group ID
    pub gid: u32,
    /// Type
    pub node_type: SFlag,
    /// For symlink
    pub link: Option<PathBuf>,
}

/// Rename parameters
#[derive(Serialize, Deserialize, Debug)]
pub struct RenameParam {
    /// Old parent directory i-number
    pub old_parent: INum,
    /// Old name
    pub old_name: String,
    /// New parent directory i-number
    pub new_parent: INum,
    /// New name
    pub new_name: String,
    /// Rename flags
    pub flags: u32,
}

/// POSIX file lock parameters
#[derive(Debug)]
pub struct FileLockParam {
    /// File handler
    pub fh: u64,
    /// Lock owner
    pub lock_owner: u64,
    /// Start offset
    pub start: u64,
    /// End offset
    pub end: u64,
    /// Lock type
    pub typ: u32,
    /// The process ID of the lock
    pub pid: u32,
}

/// File attributes
#[derive(Copy, Clone, Debug)]
pub struct FileAttr {
    /// Inode number
    pub ino: INum,
    /// Size in bytes
    pub size: u64,
    /// Size in blocks
    pub blocks: u64,
    /// Time of last access
    pub atime: SystemTime,
    /// Time of last modification
    pub mtime: SystemTime,
    /// Time of last change
    pub ctime: SystemTime,
    /// Kind of file (directory, file, pipe, etc)
    pub kind: SFlag,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    pub rdev: u32,
    /// Version
    pub version: u64,
}

/// Whether to check permission.
/// If fuse mount with `-o default_permissions`, then we should not check
/// permission. Otherwise, we should check permission.
/// TODO: add a feature flag to control this
pub const NEED_CHECK_PERM: bool = false;

impl FileAttr {
    /// New a `FileAttr`
    pub(crate) fn now() -> Self {
        let now = SystemTime::now();
        Self {
            ino: 0,
            size: 4096,
            blocks: 8,
            atime: now,
            mtime: now,
            ctime: now,
            kind: SFlag::S_IFREG,
            perm: 0o775,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            version: 0,
        }
    }

    /// Precheck before set attr
    pub(crate) fn setattr_precheck(
        &self,
        param: &SetAttrParam,
        context_uid: u32,
        context_gid: u32,
    ) -> DatenLordResult<Option<FileAttr>> {
        let cur_attr = *self;
        let mut dirty_attr = cur_attr;

        let st_now = SystemTime::now();
        let mut attr_changed = false;

        let check_permission = || -> DatenLordResult<()> {
            if NEED_CHECK_PERM {
                //  owner is root check the uid
                if cur_attr.uid == 0 && context_uid != 0 {
                    return build_error_result_from_errno(
                        Errno::EPERM,
                        "setattr() cannot change atime".to_owned(),
                    );
                }
                cur_attr.check_perm(context_uid, context_gid, 2)?;
                if context_uid != cur_attr.uid {
                    return build_error_result_from_errno(
                        Errno::EACCES,
                        "setattr() cannot change atime".to_owned(),
                    );
                }
                Ok(())
            } else {
                // We don't need to check permission
                Ok(())
            }
        };

        if let Some(gid) = param.g_id {
            if context_uid != 0 && cur_attr.uid != context_uid {
                return build_error_result_from_errno(
                    Errno::EPERM,
                    "setattr() cannot change gid".to_owned(),
                );
            }

            if cur_attr.gid != gid {
                dirty_attr.gid = gid;
                attr_changed = true;
            }
        }

        if let Some(uid) = param.u_id {
            if cur_attr.uid != uid {
                if context_uid != 0 {
                    return build_error_result_from_errno(
                        Errno::EPERM,
                        "setattr() cannot change uid".to_owned(),
                    );
                }
                dirty_attr.uid = uid;
                attr_changed = true;
            }
        }

        if let Some(mode) = param.mode {
            let mode: u16 = mode.cast();
            if mode != cur_attr.perm {
                if context_uid != 0 && context_uid != cur_attr.uid {
                    return build_error_result_from_errno(
                        Errno::EPERM,
                        "setattr() cannot change mode".to_owned(),
                    );
                }
                dirty_attr.perm = mode;
                attr_changed = true;
            }
        }

        if let Some(atime) = param.a_time {
            check_permission()?;
            if atime != cur_attr.atime {
                dirty_attr.atime = atime;
                attr_changed = true;
            }
        }

        if let Some(mtime) = param.m_time {
            check_permission()?;
            if mtime != cur_attr.mtime {
                dirty_attr.mtime = mtime;
                attr_changed = true;
            }
        }

        if let Some(file_size) = param.size {
            dirty_attr.size = file_size;
            dirty_attr.mtime = st_now;
            attr_changed = true;
        }

        if attr_changed {
            dirty_attr.ctime = st_now;
        }

        // The `ctime` can be changed implicitly, but if it's specified, just use the
        // specified one.
        #[cfg(feature = "abi-7-23")]
        if let Some(ctime) = param.c_time {
            check_permission()?;
            if ctime != cur_attr.ctime {
                dirty_attr.ctime = ctime;
                attr_changed = true;
            }
        }

        Ok(attr_changed.then_some(dirty_attr))
    }

    /// File permissions in Unix/Linux systems are represented as a 12-bit structure,
    /// laid out as follows:
    /// ┌───────────────┬─────────┬─────────┬─────────┐
    /// │   Special     │  User   │  Group  │  Other  │
    /// ├───────────────┼─────────┼─────────┼─────────┤
    /// │   3 Bits      │ 3 Bits  │ 3 Bits  │ 3 Bits  │
    /// ├───────────────┼─────────┼─────────┼─────────┤
    /// │ suid|sgid|stky│  r w x  │  r w x  │  r w x  │
    /// └──────┬───────┴────┬────┴────┬────┴────┬────┘
    ///        │             │         │         │
    ///        │             │         │         └─ Other: Read, Write, Execute permissions for other users.
    ///        │             │         └─ Group: Read, Write, Execute permissions for group members.
    ///        │             └─ User:  Read, Write, Execute permissions for the owner of the file.
    ///        └─ Special: Set User ID (suid), Set Group ID (sgid), and Sticky Bit (stky).
    /// When Sticky Bit set on a directory, files in that directory may only be unlinked or -
    /// renamed by root or the directory owner or the file owner.
    pub fn check_perm(&self, uid: u32, gid: u32, access_mode: u8) -> DatenLordResult<()> {
        if NEED_CHECK_PERM {
            self.check_perm_inner(uid, gid, access_mode)
        } else {
            Ok(())
        }
    }

    /// If `NEED_CHECK_PERM` is true, then check permission by ourselves not
    /// rely on kernel.
    #[inline]
    fn check_perm_inner(&self, uid: u32, gid: u32, access_mode: u8) -> DatenLordResult<()> {
        debug_assert!(
            access_mode <= 0o7 && access_mode != 0,
            "check_perm() found access_mode={access_mode} invalid",
        );
        if uid == 0 {
            return Ok(());
        }

        let file_mode = self.get_access_mode(uid, gid);
        debug!(
            "check_perm() got access_mode={access_mode} and file_mode={file_mode} \
            from uid={uid} gid={gid}",
        );
        if (file_mode & access_mode) != access_mode {
            return build_error_result_from_errno(
                Errno::EACCES,
                format!("check_perm() failed {uid} {gid} {file_mode}"),
            );
        }
        Ok(())
    }

    /// For given uid and gid, get the access mode of the file
    #[allow(clippy::default_numeric_fallback)]
    #[allow(clippy::arithmetic_side_effects)]
    fn get_access_mode(&self, uid: u32, gid: u32) -> u8 {
        let perm = self.perm;
        let mode = if uid == self.uid {
            (perm >> 6) & 0o7
        } else if gid == self.gid {
            (perm >> 3) & 0o7
        } else {
            perm & 0o7
        };
        mode.cast()
    }
}

impl Default for FileAttr {
    fn default() -> Self {
        Self {
            ino: 0,
            size: 4096,
            blocks: 8,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            kind: SFlag::S_IFREG,
            perm: 0o775,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            version: 0,
        }
    }
}

/// Parse `OFlag`
pub fn parse_oflag(flags: u32) -> OFlag {
    debug_assert!(
        flags < std::i32::MAX.cast::<u32>(),
        "helper_parse_oflag() found flags={flags} overflow, larger than u16::MAX",
    );
    let o_flags = OFlag::from_bits_truncate(flags.cast());
    debug!("helper_parse_oflag() read file flags={:?}", o_flags);
    o_flags
}

/// Parse file mode
pub fn parse_mode(mode: u32) -> Mode {
    debug_assert!(
        mode < std::u16::MAX.cast::<u32>(),
        "helper_parse_mode() found mode={mode} overflow, larger than u16::MAX",
    );

    #[cfg(target_os = "linux")]
    let file_mode = Mode::from_bits_truncate(mode);
    debug!("parse_mode() read mode={:?}", file_mode);
    file_mode
}

/// Parse file mode bits
#[must_use]
pub fn parse_mode_bits(mode: u32) -> u16 {
    #[cfg(target_os = "linux")]
    let bits = parse_mode(mode).bits().cast();

    bits
}

/// Convert system time to timestamp in seconds and nano-seconds
#[must_use]
pub fn time_from_system_time(system_time: &SystemTime) -> (u64, u32) {
    let duration = system_time
        .duration_since(UNIX_EPOCH)
        .context(format!(
            "failed to convert SystemTime={system_time:?} to Duration"
        ))
        .unwrap_or_else(|e| {
            panic!(
                "time_from_system_time() failed to convert SystemTime={:?} \
                to timestamp(seconds, nano-seconds), the error is: {}",
                system_time,
                util::format_anyhow_error(&e),
            )
        });
    (duration.as_secs(), duration.subsec_nanos())
}

/// Convert `FileAttr` to `FuseAttr`
#[must_use]
pub fn convert_to_fuse_attr(attr: FileAttr) -> FuseAttr {
    let (a_time_secs, a_time_nanos) = time_from_system_time(&attr.atime);
    let (m_time_secs, m_time_nanos) = time_from_system_time(&attr.mtime);
    let (c_time_secs, c_time_nanos) = time_from_system_time(&attr.ctime);

    FuseAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: a_time_secs,
        mtime: m_time_secs,
        ctime: c_time_secs,
        atimensec: a_time_nanos,
        mtimensec: m_time_nanos,
        ctimensec: c_time_nanos,
        mode: crate::async_fuse::util::mode_from_kind_and_perm(attr.kind, attr.perm),
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        #[cfg(feature = "abi-7-9")]
        blksize: 0, // TODO: find a proper way to set block size
        #[cfg(feature = "abi-7-9")]
        padding: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::assertions_on_result_states)]
    fn test_permission_check() {
        let file = FileAttr {
            ino: 0,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            kind: SFlag::S_IFREG,
            perm: 0o741,
            nlink: 0,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            version: 0,
        };

        // Owner permission checks
        assert!(file.check_perm_inner(1000, 1001, 7).is_ok());
        assert!(file.check_perm_inner(1000, 1001, 6).is_ok());
        assert!(file.check_perm_inner(1000, 1001, 5).is_ok());
        assert!(file.check_perm_inner(1000, 1001, 4).is_ok());
        assert!(file.check_perm_inner(1000, 1001, 3).is_ok());
        assert!(file.check_perm_inner(1000, 1001, 2).is_ok());
        assert!(file.check_perm_inner(1000, 1001, 1).is_ok());

        // Group permission checks
        assert!(file.check_perm_inner(1001, 1000, 7).is_err());
        assert!(file.check_perm_inner(1001, 1000, 6).is_err());
        assert!(file.check_perm_inner(1001, 1000, 5).is_err());
        assert!(file.check_perm_inner(1001, 1000, 4).is_ok());
        assert!(file.check_perm_inner(1001, 1000, 3).is_err());
        assert!(file.check_perm_inner(1001, 1000, 2).is_err());
        assert!(file.check_perm_inner(1001, 1000, 1).is_err());

        // Other permission checks
        assert!(file.check_perm_inner(1002, 1002, 7).is_err());
        assert!(file.check_perm_inner(1002, 1002, 6).is_err());
        assert!(file.check_perm_inner(1002, 1002, 5).is_err());
        assert!(file.check_perm_inner(1002, 1002, 4).is_err());
        assert!(file.check_perm_inner(1002, 1002, 3).is_err());
        assert!(file.check_perm_inner(1002, 1002, 2).is_err());
        assert!(file.check_perm_inner(1002, 1002, 1).is_ok());
    }
}
