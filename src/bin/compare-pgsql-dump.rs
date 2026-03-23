use clap::Parser;
use float_cmp::approx_eq;
use geo_types::Geometry;
use geozero::ToGeo;
use geozero::wkb::Ewkb;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::process::ExitCode;

// Represents an entry in the file
#[derive(Debug, PartialEq, Default)]
struct Entry {
    line: Vec<String>,
    tags: HashMap<String, String>,
    ewkb: Option<Geometry>,
}

// Parses a line from the file into an Entry
fn parse_line(line: &str) -> Entry {
    let ewkb_prefixes = [
        "01010000", // POINT
        "01020000", // LINESTRING
        "01030000", // POLYGON
        "01040000", // MULTIPOINT
        "01050000", // MULTILINESTRING
        "01060000", // MULTIPOLYGON
        "01070000", // GEOMETRYCOLLECTION
    ];

    let mut entry: Entry = Entry::default();

    for part in line.split("\t") {
        if part.len() > 16 && ewkb_prefixes.iter().any(|s| part.starts_with(s)) {
            let vec = hex::decode(part).unwrap();
            let ewkb = Ewkb(vec);
            entry.ewkb = Some(ewkb.to_geo().unwrap());
        } else if part.contains("=>") {
            let part = &part[1..part.len() - 1];
            for kv in part.split("\",\"") {
                let mut sp = kv.split("\"=>\"");
                let k = sp.next().unwrap().to_string();
                let v = sp.next().unwrap().to_string();
                entry.tags.insert(k, v);
            }
        } else {
            entry.line.push(part.to_string());
        }
    }

    entry
}

// Compares two entries, allowing for slight floating-point differences in EWKB
fn compare_entries(entry1: &Entry, entry2: &Entry) -> bool {
    if entry1.line != entry2.line {
        println!("  line differs:");
        println!("    {:?}", entry1.line);
        println!("    {:?}", entry2.line);
        return false;
    }

    if entry1.tags.len() != entry2.tags.len() {
        return false;
    }

    for (key, value) in &entry1.tags {
        if !entry2.tags.contains_key(key) || entry2.tags[key] != *value {
            return false;
        }
    }
    if let Some(e1) = &entry1.ewkb
        && let Some(e2) = &entry2.ewkb
    {
        match (e1, e2) {
            (Geometry::Point(p1), Geometry::Point(p2)) => {
                if !approx_eq!(f64, p1.x(), p2.x(), epsilon = 0.00000000000001)
                    || !approx_eq!(f64, p1.y(), p2.y(), epsilon = 0.00000000000001)
                {
                    println!(
                        "point differs: {} {} / {} {}",
                        p1.x(),
                        p2.x(),
                        p1.y(),
                        p2.y()
                    );
                    return false;
                }
            }
            (Geometry::LineString(l1), Geometry::LineString(l2)) => {
                let v1 = &l1.0;
                let v2 = &l2.0;
                if v1.len() != v2.len() {
                    println!(
                        "  linestring differs on length: {} / {}",
                        v1.len(),
                        v2.len()
                    );
                    println!("    {:?}", v1);
                    println!("    {:?}", v2);
                    return false;
                }
                for pos in 0..v1.len() {
                    let p1 = v1[pos];
                    let p2 = v2[pos];
                    if !approx_eq!(f64, p1.x, p2.x, epsilon = 0.00000000000001)
                        || !approx_eq!(f64, p1.y, p2.y, epsilon = 0.00000000000001)
                    {
                        println!(
                            "linestring point differs: {} {} / {} {}",
                            p1.x, p2.x, p1.y, p2.y
                        );
                        return false;
                    }
                }
            }
            _ => return false,
        }
    } else if entry1.ewkb != entry2.ewkb {
        return false;
    }

    true
}

// Main function to compare two files
fn compare_files(file1: &str, file2: &str) -> bool {
    let file1 = File::open(file1).expect("Unable to open file1");
    let file2 = File::open(file2).expect("Unable to open file2");

    let reader1 = BufReader::new(file1);
    let reader2 = BufReader::new(file2);

    let mut num_errors = 0;

    let mut line_number = 1;
    let mut lines1 = reader1.lines();
    let mut lines2 = reader2.lines();

    loop {
        let l1 = lines1.next();
        let l2 = lines2.next();
        if let Some(Ok(ref l1)) = l1
            && let Some(Ok(ref l2)) = l2
        {
            let e1 = parse_line(l1);
            let e2 = parse_line(l2);
            if !compare_entries(&e1, &e2) {
                println!("Difference line {line_number}");
                println!("  {l1}");
                println!("  {l2}");
                num_errors += 1;
                if num_errors > 3 {
                    println!("... Stopping comparison as error threshold reached");
                    return false;
                }
            }
        } else if l1.is_none() && l2.is_none() {
            // End of both files
            return num_errors != 0;
        } else {
            println!("Files have a different number of lines - {line_number}");
            println!("  {:?}", l1);
            println!("  {:?}", l2);
            return false;
        }
        line_number += 1;
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    pub file1: String,
    pub file2: String,
}

#[cfg(feature = "enable-compare-pgsql-dump")]
fn main() -> ExitCode {
    let args = Args::parse();

    if !compare_files(&args.file1, &args.file2) {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
