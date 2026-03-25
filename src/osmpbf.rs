//! Reader for OpenStreetMap pbf files

use osmpbfreader;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use time::macros::format_description;

use crate::osm::{OsmCopyTo, OsmWriter};

/// Reader for OpenStreetMap pbf files
pub struct OsmPbf {
    filename: String,
}

impl OsmPbf {
    /// Read a pbf file
    pub fn new(filename: &str) -> Result<OsmPbf, Box<dyn Error>> {
        Ok(OsmPbf {
            filename: filename.to_string(),
        })
    }
}

macro_rules! eprintlnt {
    ($($arg:tt)*) => {
        eprintln!("{} {}",
                 time::OffsetDateTime::now_local()
                    .unwrap_or_else(|_| time::OffsetDateTime::now_utc())
                    .format(&format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"))
                    .unwrap(),
                 format_args!($($arg)*)
                );
    };
}

impl<T> OsmCopyTo<T> for OsmPbf
where
    T: OsmWriter,
{
    #[allow(clippy::cast_sign_loss)]
    fn copy_to(&mut self, target: &mut T) -> Result<(), Box<dyn Error>> {
        let r = match File::open(Path::new(&self.filename)) {
            Err(e) => {
                let red = anstyle::Style::new().fg_color(Some(anstyle::AnsiColor::Red.into()));
                eprintln!(
                    "{red}Error: Please put a valid pbf file on {0}{red:#}",
                    self.filename
                );
                return Err(Box::new(e));
            }
            Ok(o) => o,
        };
        let mut pbf = osmpbfreader::OsmPbfReader::new(r);

        target.write_start(false).unwrap();
        let mut start_way = false;
        let mut start_relation = false;

        eprintlnt!("Starting pbf read");

        for obj in pbf.par_iter() {
            let obj = obj?;
            match obj {
                osmpbfreader::OsmObj::Node(node) => {
                    target.write_node(&node).unwrap();
                }
                osmpbfreader::OsmObj::Way(way) => {
                    if !start_way {
                        eprintlnt!("Starting ways");
                        start_way = true;
                    }
                    target.write_way(&way).unwrap();
                }
                osmpbfreader::OsmObj::Relation(relation) => {
                    if !start_relation {
                        eprintlnt!("Starting relations");
                        start_relation = true;
                    }
                    target.write_relation(&relation).unwrap();
                }
            }
        }
        eprintlnt!("Finished pbf read");

        target.write_end(false).unwrap();

        Ok(())
    }
}
