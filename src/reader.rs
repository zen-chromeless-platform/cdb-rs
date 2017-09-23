extern crate mmap_fixed;

use std::ffi::OsStr;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::cmp::min;
use std::ptr;
use std::slice;

use std::path::Path;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

use hash::hash;
use uint32::*;

use self::mmap_fixed::{MemoryMap, MapOption};

pub use std::io::Result;

const KEY_SIZE: usize = 32;

#[cfg(unix)]
#[inline(always)]
fn get_fd(file: &fs::File) -> ::std::os::unix::io::RawFd {
    file.as_raw_fd()
}

#[cfg(windows)]
#[inline(always)]
fn get_fd(file: &fs::File) -> ::std::os::windows::io::RawHandle {
    file.as_raw_handle()
}

fn mmap_file(file: &fs::File, len: usize) -> Result<MemoryMap> {
    match MemoryMap::new(len, &[ MapOption::MapReadable, MapOption::MapFd(get_fd(file)) ]) {
        Err(_) =>
            Err(io::Error::new(io::ErrorKind::Other, "mmap failed")),
        Ok(x) =>
            Ok(x),
    }
}

#[inline(always)]
fn invalid_file_format_err<T>() -> Result<T> {
    Err(io::Error::new(io::ErrorKind::Other, "Invalid file format"))
}

/// CDB file reader
pub struct CdbReader {
    file: io::BufReader<fs::File>,
    size: usize,
    pos: u32,
    mmap: Option<MemoryMap>,
    header: [u8; 2048],
}

impl CdbReader {

    /// Constructs a new CDB reader from an already opened file.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::fs;
    ///
    /// let file = fs::File::open("tests/test1.cdb").unwrap();
    /// let cdb = cdb::CdbReader::new(file).unwrap();
    /// ```
    pub fn new(file: fs::File) -> Result<Self> {
        let mut header = [0; 2048];

        let meta = file.metadata()?;
        let mut file = io::BufReader::new(file);
        if meta.len() < 2048 + 8 + 8 || meta.len() > 0xffffffff {
            return invalid_file_format_err();
        }

        let mmap = if let Ok(m) = mmap_file(&file.get_ref(), meta.len() as usize) {
            Some(m)
        } else {
            file.seek(io::SeekFrom::Start(0))?;
            file.read(&mut header)?;
            None
        };

        let pos = 2048;
        let size = meta.len() as usize;
        Ok(CdbReader { file, header, pos, size, mmap })
    }

    /// Constructs a new CDB by opening a file.
    ///
    /// # Examples
    ///
    /// ```
    /// let cdb = cdb::CdbReader::open("tests/test1.cdb").unwrap();
    /// ```
    pub fn open<S: AsRef<OsStr> + ?Sized>(file_name: &S) -> Result<Self> {
        let file = fs::File::open(Path::new(file_name))?;
        CdbReader::new(file)
    }
}

impl CdbReader {

    /// Find all records with the named key.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut cdb = cdb::CdbReader::open("tests/test1.cdb").unwrap();
    ///
    /// for result in cdb.find(b"one") {
    ///     println!("{:?}", result.unwrap());
    /// }
    /// ```
    #[inline]
    pub fn find(&mut self, key: &[u8]) -> CdbIter {
        CdbIter::find(self, key)
    }

    fn read(&mut self, buf: &mut [u8], pos: u32) -> Result<usize> {
        if pos as usize + buf.len() > self.size {
            return invalid_file_format_err();
        }

        if let Some(ref map) = self.mmap {
            unsafe {
                ptr::copy_nonoverlapping(map.data().offset(pos as isize), buf.as_mut_ptr(), buf.len());
            }
            Ok(buf.len())
        } else {
            if pos != self.pos {
                self.file.seek(io::SeekFrom::Start(pos as u64))?;
            }

            let mut len = buf.len();
            let mut read = 0;
            while len > 0 {
                let r = self.file.read(&mut buf[read..])?;
                len -= r;
                read += r;
            }
            Ok(read)
        }
    }

    fn hash_table(&self, khash: u32) -> (u32, u32, u32) {
        let x = ((khash as usize) & 0xff) << 3;

        let (hpos, hslots) = if let Some(ref map) = self.mmap {
            let s = unsafe { slice::from_raw_parts(map.data(), 2048) };
            uint32_unpack2(&s[x..x+8])
        } else {
            uint32_unpack2(&self.header[x..x+8])
        };

        let kpos = if hslots > 0 { hpos + (((khash >> 8) % hslots) << 3) } else { 0 };
        (hpos, hslots, kpos)
    }

    fn match_key(&mut self, key: &[u8], pos: u32) -> Result<bool> {
        let mut buf = [0 as u8; KEY_SIZE];
        let mut len = key.len();
        let mut pos = pos;
        let mut keypos = 0;

        while len > 0 {
            let n = min(len, buf.len());
            self.read(&mut buf[..n], pos)?;
            if buf[..n] != key[keypos..keypos + n] {
                return Ok(false);
            }
            pos += n as u32;
            keypos += n;
            len -= n;
        }
        Ok(true)
    }
}

pub struct CdbIter<'a> {
    cdb: &'a mut CdbReader,
    key: Vec<u8>,
    khash: u32,
    kloop: u32,
    kpos: u32,
    hpos: u32,
    hslots: u32,
    dpos: u32,
    dlen: u32,
}

impl<'a> CdbIter<'a> {
    fn find(cdb: &'a mut CdbReader, key: &[u8]) -> Self {
        let khash = hash(key);
        let (hpos, hslots, kpos) = cdb.hash_table(khash);
        let key = key.into_iter().map(|x| *x).collect();

        CdbIter { cdb, key, khash, kloop: 0, kpos, hpos, hslots, dpos: 0, dlen: 0, }
    }
}

impl<'a> CdbIter<'a> {
    fn read_vec(&mut self) -> Result<Vec<u8>> {
        let mut result = vec![0; self.dlen as usize];
        self.cdb.read(&mut result[..], self.dpos)?;
        Ok(result)
    }
}

macro_rules! iter_try {
    ( $e:expr ) => {
        match $e {
            Err(x) => {
                return Some(Err(x));
            },
            Ok(y) =>
                y
        }
    }
}

impl<'a> Iterator for CdbIter<'a> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.kloop < self.hslots {
            let mut buf = [0 as u8; 8];
            let kpos = self.kpos;

            iter_try!(self.cdb.read(&mut buf, kpos));
            let (khash, pos) = uint32_unpack2(&buf);

            if pos == 0 { return None; }

            self.kloop += 1;
            self.kpos += 8;
            if self.kpos == self.hpos + (self.hslots << 3) {
                self.kpos = self.hpos;
            }

            if khash == self.khash {
                iter_try!(self.cdb.read(&mut buf, pos));

                let (klen, dlen) = uint32_unpack2(&buf);
                if klen as usize == self.key.len() {
                    if iter_try!(self.cdb.match_key(&self.key[..], pos + 8)) {

                        self.dlen = dlen;
                        self.dpos = pos + 8 + self.key.len() as u32;
                        return Some(self.read_vec());
                    }
                }
            }
        }
        None
    }
}
