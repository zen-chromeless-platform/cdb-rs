extern crate cdb;

#[test]
fn one_read() {
    let mut cdb = cdb::CdbReader::open("tests/test1.cdb").unwrap();
    let mut i = cdb.find(b"one");
    assert_eq!(i.next().unwrap().unwrap(), b"Hello");
    assert_eq!(i.next().unwrap().unwrap(), b", World!");
}


#[test]
fn two_reads() {
    let mut cdb = cdb::CdbReader::open("tests/test1.cdb").unwrap();
    assert_eq!(cdb.find(b"two").next().unwrap().unwrap(), b"Goodbye");
    assert_eq!(cdb.find(b"this key will be split across two reads").next().unwrap().unwrap(), b"Got it.");
}
