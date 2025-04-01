//! Basic handling of OpenStreetMap data

use osmpbfreader::objects::{Node, Relation, Way};
use std::error::Error;
use std::fmt;
use std::io;

use crate::osmpbf;
//use crate::osmxml;

#[allow(clippy::cast_possible_truncation)]
/// Convert a floating-point latitude/longitude to the decimicro format
pub fn coord_to_decimicro(coord: f64) -> i32 {
    (coord * 1e7).round() as i32
}
/// Convert a decimicro latitude/longitude to floating-point
pub fn decimicro_to_coord(decimicro: i32) -> f64 {
    f64::from(decimicro) * 1e-7
}

/// Action to apply to an Element
#[derive(Clone, PartialEq)]
pub enum Action {
    Create(),
    Modify(),
    Delete(),
    None,
}

/// Reader returning a node/way/relation from an osm id
pub trait OsmReader {
    fn read_node(&mut self, id: u64) -> Option<Node>;
    fn read_way(&mut self, id: u64) -> Option<Way>;
    fn read_relation(&mut self, id: u64) -> Option<Relation>;
}

/// Writer writing a new node/way/relation
pub trait OsmWriter {
    fn write_node(&mut self, node: &Node) -> Result<(), io::Error>;
    fn write_way(&mut self, way: &Way) -> Result<(), io::Error>;
    fn write_relation(&mut self, relation: &Relation) -> Result<(), io::Error>;

    fn write_start(&mut self, _change: bool) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
    fn write_end(&mut self, _change: bool) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn import(&mut self, filename: &str) -> Result<(), Box<dyn Error>>
    where
        Self: Sized,
    {
        if filename.ends_with(".pbf") {
            let mut reader = osmpbf::OsmPbf::new(filename).unwrap();
            reader.copy_to(self)
        } else {
            Err(NotSupportedFileType {
                filename: filename.to_string(),
            }
            .into())
        }
    }
}

pub trait OsmCopyTo<T: OsmWriter> {
    fn copy_to(&mut self, target: &mut T) -> Result<(), Box<dyn Error>>;
}

#[derive(Debug)]
pub struct NotSupportedFileType {
    pub filename: String,
}
impl Error for NotSupportedFileType {}
impl fmt::Display for NotSupportedFileType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "File {} is not supported", self.filename)
    }
}
