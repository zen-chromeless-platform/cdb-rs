use std::fs;
use std::io;
use std::io::prelude::*;
use std::cmp::max;
use std::iter;

use super::Result;
use hash::hash;
use uint32::*;

type Hash = u32;
type Pos = u32;

#[inline(always)]
fn file_too_big_err<T>() -> Result<T> {
    Err(io::Error::new(io::ErrorKind::Other, "File too big"))
}

/// Base interface for making a CDB file.
pub struct CdbMaker {
    entries: Vec<Vec<(Hash, Pos)>>,
    pos: u32,
    file: io::BufWriter<fs::File>,
}

impl CdbMaker {

    /// Create a new CDB maker.
    pub fn new(file: fs::File) -> Result<Self> {
        let mut file = io::BufWriter::new(file);
        let buf = [0; 2048];
        file.seek(io::SeekFrom::Start(0))?;
        file.write(&buf)?;

        Ok(CdbMaker { entries: iter::repeat(vec![]).take(256).collect::<Vec<_>>(), pos: 2048, file })
    }
}

impl super::CdbWrite for CdbMaker {

    fn insert(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        if key.len() >= 0xffffffff || data.len() >= 0xffffffff {
            Err(io::Error::new(io::ErrorKind::Other, "Key or data too big"))
        } else {
            self.insert_start(key.len() as u32, data.len() as u32)?;
            self.file.write(key)?;
            self.file.write(data)?;
            self.insert_end(key.len() as u32, data.len() as u32, hash(&key[..]))
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut buf = [0; 8];

        let maxsize = self.entries.iter().fold(1, |acc, e| max(acc, e.len() * 2));
        let count = self.entries.iter().fold(0, |acc, e| acc + e.len());

        if maxsize + count > (0xffffffff / 8) {
            return file_too_big_err();
        }

        let mut table = vec![(0 as Hash, 0 as Pos); maxsize];

        let mut header = [0 as u8; 2048];
        for (i, j) in (0..256).map(|it| it * 8).enumerate() {
            let len = self.entries[i].len() * 2;
            uint32_pack2(&mut header[j..j + 8], self.pos, len as u32);

            for e in self.entries[i].iter() {
                let mut wh = (e.0 as usize >> 8) % len;
                while table[wh].1 != 0 {
                    wh += 1;
                    if wh == len { wh = 0; }
                }
                table[wh] = *e;
            }

            for hp in table.iter_mut().take(len) {
                uint32_pack2(&mut buf, hp.0, hp.1);
                self.file.write(&buf)?;
                self.pos_plus(8)?;
                *hp = (0 as Hash, 0 as Pos);
            }
        }

        self.file.flush()?;
        self.file.seek(io::SeekFrom::Start(0))?;
        self.file.write(&header)?;
        self.file.flush()?;
        Ok(())
    }
}

fn pos_plus(pos: u32, len: u32) -> Result<(u32)> {
    if pos + len < len { file_too_big_err() } else { Ok(pos + len) }
}

impl CdbMaker {

    // XXX: この関数内で自分自身を書き換える必要はない
    // 戻り値を返せばいいだけ
    fn pos_plus(&mut self, len: u32) -> Result<()> {
        if self.pos + len < len {
            file_too_big_err()
        } else {
            self.pos += len;
            Ok(())
        }
    }

    fn insert_start(&mut self, key_len: u32, data_len: u32) -> Result<()> {
        let mut buf = [0; 8];
        uint32_pack2(&mut buf[..], key_len, data_len);
        self.file.write(&buf)?;
        Ok(())
    }

    fn insert_end(&mut self, key_len: u32, data_len: u32, hash: u32) -> Result<()> {
        self.entries[(hash & 0xff) as usize].push((hash as Hash, self.pos as Pos));
        self.pos_plus(8)?;
        self.pos_plus(key_len)?;
        self.pos_plus(data_len)?;
        Ok(())
    }
}
