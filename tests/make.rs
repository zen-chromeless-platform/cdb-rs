extern crate cdb;

use std::fs;

use cdb::CdbWrite;

#[test]
fn make() {
    let filename = "tests/make.cdb";

    {
        let mut cdb = cdb::CdbWriter::new(filename, "tests/make.cdb.tmp").unwrap();
        cdb.insert(b"one", b"Hello").unwrap();
        cdb.insert(b"two", b"Goodbye").unwrap();
        cdb.insert(b"one", b", World!").unwrap();
        cdb.insert(b"this key will be split across two reads", b"Got it.").unwrap();
        cdb.flush().unwrap();

        let mut cdb = cdb::CdbReader::open(filename).unwrap();
        assert_eq!(cdb.find(b"two").next().unwrap().unwrap(), b"Goodbye");
        assert_eq!(cdb.find(b"this key will be split across two reads").next().unwrap().unwrap(), b"Got it.");
        let mut i = cdb.find(b"one");
        assert_eq!(i.next().unwrap().unwrap(), b"Hello");
        assert_eq!(i.next().unwrap().unwrap(), b", World!");
    }

    fs::remove_file(filename).unwrap();
}
