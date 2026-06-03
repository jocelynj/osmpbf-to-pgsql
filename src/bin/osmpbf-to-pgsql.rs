use clap::Parser;
use std::error::Error;

use osmpbf_to_pgsql::osm::OsmWriter;
use osmpbf_to_pgsql::postgres;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, help = "Osm PBF file to import")]
    pbf: String,
    #[arg(
        long,
        help = "Dump changes to files in specified directory to use with COPY"
    )]
    dump: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut db = postgres::Postgres::new(&args.dump);
    db.import(&args.pbf).unwrap();
    Ok(())
}
