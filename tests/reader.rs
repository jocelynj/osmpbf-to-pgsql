use flate2::read::GzDecoder;
use osmpbf_to_pgsql::osm::OsmWriter;
use osmpbf_to_pgsql::postgres;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;

const PBF: &str = "tests/clipperton.osm.pbf";
const DUMP_ORIG: &str = "tests/clipperton-dump/";

fn open_and_decompress(path: &str) -> Result<Box<dyn Read>, std::io::Error> {
    let file = File::open(path)?;

    if path.ends_with(".gz") {
        Ok(Box::new(GzDecoder::new(file)))
    } else {
        Ok(Box::new(file))
    }
}

fn compare_files(file1: &str, file2: &str) -> bool {
    println!("Comparing {file1} and {file2}");
    let f1 = open_and_decompress(file1).unwrap();
    let f2 = open_and_decompress(file2).unwrap();

    let mut f1 = BufReader::new(f1);
    let mut f2 = BufReader::new(f2);
    let mut pos = 0;

    loop {
        let mut bytes1 = Vec::with_capacity(1024);
        let mut bytes2 = Vec::with_capacity(1024);

        let len1 = f1.by_ref().take(1024).read_to_end(&mut bytes1).unwrap();
        let len2 = f2.by_ref().take(1024).read_to_end(&mut bytes2).unwrap();

        if len1 != len2 {
            println!(
                "Difference of file length: {} vs {}",
                pos + len1,
                pos + len2
            );
            return false;
        }
        if len1 == 0 {
            return true;
        }

        for i in 0..len1 {
            if bytes1[i] != bytes2[i] {
                println!(
                    "Difference detected at position {pos}: '{}' vs '{}'",
                    (bytes1[i] as char).escape_default(),
                    (bytes2[i] as char).escape_default()
                );
                return false;
            }
            pos += 1;
        }
    }
}

#[test]
fn pbf_to_pgdump() {
    let dumpdir_path = tempfile::tempdir().unwrap();
    let dumpdir = dumpdir_path.path().to_str().unwrap();
    let mut db = postgres::Postgres::new("", None, false, dumpdir);
    db.import(PBF).unwrap();
    drop(db);

    let files = [
        "nodes.txt",
        "ways.txt",
        "way_nodes.txt",
        "relations.txt",
        "relation_members.txt",
        "users.txt",
    ];
    for f in files.iter() {
        let orig_file = DUMP_ORIG.to_string() + *f + ".gz";
        let gen_file = dumpdir.to_string() + "/" + *f;
        assert!(compare_files(&orig_file, &gen_file));
    }
}
