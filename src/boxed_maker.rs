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
fn sink_too_big_err<T>() -> Result<T> {
    Err(io::Error::new(io::ErrorKind::Other, "File too big"))
}

/// onmemory CdbMaker.
pub struct CdbMakerBoxed {
    entries: Vec<Vec<(Hash, Pos)>>,
    pos: u32,
    sink: io::BufWriter<io::Cursor<Vec<u8>>>,
}

impl CdbMakerBoxed {

    /// Create a new CDB maker.
    pub fn new() -> Result<Self> {
        let mut sink = io::BufWriter::new(io::Cursor::new(vec![0;2048]));
        let buf = [0; 2048];
        sink.seek(io::SeekFrom::Start(0))?;
        sink.write(&buf)?;

        Ok(CdbMakerBoxed { entries: iter::repeat(vec![]).take(256).collect::<Vec<_>>(), pos: 2048, sink })
    }
}

impl super::CdbWrite for CdbMakerBoxed {

    fn insert(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        if key.len() >= 0xffffffff || data.len() >= 0xffffffff {
            Err(io::Error::new(io::ErrorKind::Other, "Key or data too big"))
        } else {
            self.insert_start(key.len() as u32, data.len() as u32)?;
            self.sink.write(key)?;
            self.sink.write(data)?;
            self.insert_end(key.len() as u32, data.len() as u32, hash(&key[..]))
        }
    }

    fn flush(&mut self) -> Result<()> {
        let mut buf = [0; 8];

        let maxsize = self.entries.iter().fold(1, |acc, e| max(acc, e.len() * 2));
        let count = self.entries.iter().fold(0, |acc, e| acc + e.len());

        if maxsize + count > (0xffffffff / 8) {
            return sink_too_big_err();
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
                self.sink.write(&buf)?;
                self.pos_plus(8)?;
                *hp = (0 as Hash, 0 as Pos);
            }
        }

        self.sink.flush()?;
        self.sink.seek(io::SeekFrom::Start(0))?;
        self.sink.write(&header)?;
        self.sink.flush()?;
        Ok(())
    }
}

impl CdbMakerBoxed {

    pub fn write_all<W: io::Write>(self, writer: &mut W) -> super::Result<()> {
        let inner = self.sink.get_ref().get_ref().as_slice();
        writer.write_all(inner)
    }

    fn pos_plus(&mut self, len: u32) -> Result<()> {
        if self.pos + len < len {
            sink_too_big_err()
        } else {
            self.pos += len;
            Ok(())
        }
    }

    fn insert_start(&mut self, key_len: u32, data_len: u32) -> Result<()> {
        let mut buf = [0; 8];
        uint32_pack2(&mut buf[..], key_len, data_len);
        self.sink.write(&buf)?;
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
