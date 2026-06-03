use clap::Parser;
use std::error::Error;

use osmpbf_to_pgsql::osm::OsmWriter;
use osmpbf_to_pgsql::postgres;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(
        long,
        help = "Connect string to Postgresql database",
        default_value = "host=/run/postgresql/"
    )]
    psql: String,
    #[arg(long, help = "Osm PBF file to import")]
    pbf: Option<String>,
    #[arg(
        long,
        help = "Dump changes to files in specified directory to use with COPY"
    )]
    dump: String,
    #[arg(long, help = "Postgresql schema to use")]
    schema: Option<String>,
    #[arg(long, help = "Initialize tables")]
    init: bool,
    #[arg(long, help = "Truncate tables before import")]
    truncate: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut db = postgres::Postgres::new(&args.psql, args.schema, args.init, &args.dump);
    if args.truncate {
        db.truncate();
    }
    if let Some(pbf) = args.pbf {
        db.import(&pbf).unwrap();
    }
    Ok(())
}
