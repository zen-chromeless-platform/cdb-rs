extern crate cdb;

use std::io::{Write, BufWriter};
use std::fs::{self, File};
use cdb::CdbWrite;

#[test]
fn boxed_make() {
    let filename = "tests/make.cdb";

    {
        let mut cdb = cdb::CdbMakerBoxed::new().unwrap();
        cdb.insert(b"one", b"Hello").unwrap();
        cdb.insert(b"two", b"Goodbye").unwrap();
        cdb.insert(b"one", b", World!").unwrap();
        cdb.insert(b"this key will be split across two reads", b"Got it.").unwrap();
        cdb.flush().unwrap();

        let mut file = BufWriter::new(File::create(filename).unwrap());
        cdb.write_all(&mut file).unwrap();
        file.flush().unwrap();

        let mut cdb = cdb::CdbReader::open(filename).unwrap();
        assert_eq!(cdb.find(b"two").next().unwrap().unwrap(), b"Goodbye");
        assert_eq!(cdb.find(b"this key will be split across two reads").next().unwrap().unwrap(), b"Got it.");
        let mut i = cdb.find(b"one");
        assert_eq!(i.next().unwrap().unwrap(), b"Hello");
        assert_eq!(i.next().unwrap().unwrap(), b", World!");
    }

    fs::remove_file(filename).unwrap();
}
