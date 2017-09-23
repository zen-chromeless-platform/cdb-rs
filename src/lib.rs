mod uint32;
mod hash;
mod reader;
mod maker;
mod boxed_maker;
mod writer;

pub trait CdbWrite {
    /// Add a record to the CDB file.
    fn insert(&mut self, key: &[u8], data: &[u8]) -> Result<()>;

    /// Finish writing to the CDB file and flush its contents.
    fn flush(&mut self) -> Result<()>;
}


pub use hash::hash;
pub use reader::{CdbReader, CdbIter};
pub use maker::CdbMaker;
pub use boxed_maker::CdbMakerBoxed;
pub use writer::CdbWriter;

pub use std::io::Result;
