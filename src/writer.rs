
use std::ffi::{OsString, OsStr};
use std::fs;

use super::Result;
use maker::CdbMaker;

/// A CDB file writer which handles atomic updating.
///
/// Using this type, a CDB file is safely written by first creating a
/// temporary file, building the CDB structure into that temporary file,
/// and finally renaming that temporary file over the final file name.
/// If the temporary file is not properly finished (ie due to an error),
/// the temporary file is deleted when this writer is dropped.
pub struct CdbWriter {
    dst_name: OsString,
    temp_name: OsString,
    cdb: CdbMaker,
}

impl CdbWriter {

    /// Safely create a new CDB file, using two specific file names.
    ///
    /// Note that the temporary file name must be on the same filesystem
    /// as the destination, or else the final rename will fail.
    pub fn new<P, Q>(file_name: P, temp_name: Q) -> Result<Self>
        where P: AsRef<OsStr>,
              Q: AsRef<OsStr>
    {
        let dst_name = file_name.as_ref().to_os_string();
        let temp_name = temp_name.as_ref().to_os_string();
        let file = fs::File::create(&temp_name)?;
        let cdb = CdbMaker::new(file)?;
        Ok(CdbWriter { dst_name, temp_name, cdb })
    }
}

impl super::CdbWrite for CdbWriter {

    #[inline(always)]
    fn insert(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        self.cdb.insert(key, data)
    }

    fn flush(&mut self) -> Result<()> {
        self.cdb.flush()?;
        fs::rename(&self.temp_name, &self.dst_name)?;
        self.temp_name.clear();
        Ok(())
    }
}

impl CdbWriter {

    /// Set permissions on the temporary file.
    ///
    /// This must be done before the file is finished, as the temporary
    /// file will no longer exist at that point.
    #[inline(always)]
    pub fn set_permissions(&mut self, perm: fs::Permissions) -> Result<()> {
        // This should be a method on the file itself to use fchmod, but
        // Rust doesn't have that yet.
        fs::set_permissions(&self.temp_name, perm)
    }
}

impl Drop for CdbWriter {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if self.temp_name.len() > 0 {
            fs::remove_file(&self.temp_name);
        }
    }
}
