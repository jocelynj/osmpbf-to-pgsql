//! Basic handling of OpenStreetMap data

use osmpbfreader::objects::{Info, Node, Relation, Way};
use std::error::Error;
use std::fmt;
use std::io;

use crate::osmpbf;

/// Writer writing a new node/way/relation
pub trait OsmWriter {
    fn write_node(&mut self, node: &Node, info: &Info) -> Result<(), io::Error>;
    fn write_way(&mut self, way: &Way, info: &Info) -> Result<(), io::Error>;
    fn write_relation(&mut self, relation: &Relation, info: &Info) -> Result<(), io::Error>;

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
