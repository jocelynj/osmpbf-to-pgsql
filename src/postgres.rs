//! Access postgres+postgis database

use geo_types::Coord;
use osmpbfreader::objects::{Node, Relation, Tags, Way};
use postgres::{Client, NoTls};
use rustc_hash::FxHashMap;
use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};
use std::path::Path;
use time::OffsetDateTime;
use time::macros::format_description;

use crate::osm::OsmWriter;

struct Statements {
    client: Client,
    schema: String,
}

struct Copy {
    nodes: BufWriter<File>,
    ways: BufWriter<File>,
    way_nodes: BufWriter<File>,
    relations: BufWriter<File>,
    relation_members: BufWriter<File>,
    users: BufWriter<File>,
}

pub struct Postgres {
    statements: Option<Statements>,
    copy: Copy,
    nodes: FxHashMap<i64, Coord>,
    users: FxHashMap<i32, smartstring::alias::String>,

    line_buffer: Vec<u8>,
    time_format: time::format_description::StaticFormatDescription,
}

impl Postgres {
    pub fn new(connect: &str, schema: Option<String>, init_tables: bool, copy_dir: &str) -> Self {
        let statements = if init_tables {
            let mut client = Client::connect(connect, NoTls).unwrap();
            if init_tables {
                Postgres::init_tables(&mut client, &schema);
            }
            let schema = if let Some(mut s) = schema {
                s.push('.');
                s
            } else {
                String::from("")
            };

            Some(Statements { client, schema })
        } else {
            None
        };
        let nodes = BufWriter::new(File::create(Path::new(copy_dir).join("nodes.txt")).unwrap());
        let ways = BufWriter::new(File::create(Path::new(copy_dir).join("ways.txt")).unwrap());
        let way_nodes =
            BufWriter::new(File::create(Path::new(copy_dir).join("way_nodes.txt")).unwrap());
        let relations =
            BufWriter::new(File::create(Path::new(copy_dir).join("relations.txt")).unwrap());
        let relation_members =
            BufWriter::new(File::create(Path::new(copy_dir).join("relation_members.txt")).unwrap());
        let users = BufWriter::new(File::create(Path::new(copy_dir).join("users.txt")).unwrap());
        let copy = Copy {
            nodes,
            ways,
            way_nodes,
            relations,
            relation_members,
            users,
        };

        Self {
            copy,
            statements,
            nodes: FxHashMap::with_capacity_and_hasher(500000, Default::default()),
            users: FxHashMap::with_capacity_and_hasher(5000, Default::default()),
            line_buffer: Vec::with_capacity(1500), // big enough to store a complex node
            time_format: format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory][offset_minute]"
            ),
        }
    }

    pub fn init_tables(client: &mut Client, schema: &Option<String>) {
        let schema_sql = include_str!("../schema.sql");
        let mut transaction = client.transaction().unwrap();
        if let Some(schema) = schema {
            transaction
                .execute(&format!("SET search_path TO {schema},public"), &[])
                .unwrap();
        }
        transaction.batch_execute(schema_sql).unwrap();
        transaction.commit().unwrap();
    }

    pub fn truncate(&mut self) {
        let statements = self.statements.as_mut().unwrap();
        let client = &mut statements.client;
        let schema = statements.schema.clone();
        client
            .execute(&format!("TRUNCATE {}nodes", schema), &[])
            .unwrap();
        client
            .execute(&format!("TRUNCATE {}ways", schema), &[])
            .unwrap();
        client
            .execute(&format!("TRUNCATE {}way_nodes", schema), &[])
            .unwrap();
        client
            .execute(&format!("TRUNCATE {}relations", schema), &[])
            .unwrap();
        client
            .execute(&format!("TRUNCATE {}relation_members", schema), &[])
            .unwrap();
        client
            .execute(&format!("TRUNCATE {}users", schema), &[])
            .unwrap();
    }

    pub fn to_hex_string(bytes: &[u8], output: &mut Vec<u8>) {
        const HEX_CHARS: &[u8; 16] = b"0123456789ABCDEF";
        for &b in bytes {
            output.push(HEX_CHARS[(b >> 4) as usize]);
            output.push(HEX_CHARS[(b & 0xF) as usize]);
        }
    }

    fn lonlat_to_ewkb(lon: f64, lat: f64, output: &mut Vec<u8>) {
        output.extend(b"0101000020E6100000"); // beginning of ewkb point string

        Self::to_hex_string(&lon.to_le_bytes(), output);
        Self::to_hex_string(&lat.to_le_bytes(), output);
    }
    fn linestring_to_ewkb(coords: &[Coord], output: &mut Vec<u8>) {
        output.extend(b"0102000020E6100000"); // beginning of ewkb linestring string
        Self::to_hex_string(&(coords.len() as u32).to_le_bytes(), output);

        for c in coords {
            Self::to_hex_string(&c.x.to_le_bytes(), output);
            Self::to_hex_string(&c.y.to_le_bytes(), output);
        }
    }

    fn way_to_ewkb(nodes: Vec<Coord>, output: &mut Vec<u8>) {
        match nodes.len() {
            0 => write!(output, "\\N").unwrap(),
            _ => Self::linestring_to_ewkb(&nodes, output),
        }
    }

    pub fn ids_to_vec(ids: &[i64], output: &mut Vec<u8>) {
        write!(output, "{{").unwrap();
        let mut iter = ids.iter();
        // First item is special, so that we don't need to remove "," at the end
        if let Some(id) = iter.next() {
            itoap::write_to_vec(output, *id);
        }
        for id in iter {
            write!(output, ",").unwrap();
            itoap::write_to_vec(output, *id);
        }
        write!(output, "}}").unwrap();
    }

    pub fn escape_string(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                _ => out.push(c),
            }
        }
        out
    }

    pub fn escape_key_value(s: &str, output: &mut Vec<u8>) {
        // Differs from escape_string() as " is escaped with 2 slashes instead of 0.
        for c in s.chars() {
            match c {
                '\\' => output.extend(b"\\\\"),
                '\n' => output.extend(b"\\n"),
                '\r' => output.extend(b"\\r"),
                '\t' => output.extend(b"\\t"),
                '"' => output.extend(b"\\\\\""),
                _ => {
                    let pos = output.len();
                    let len = c.len_utf8();
                    output.reserve(len);
                    let spare = output.spare_capacity_mut();
                    // We are initialising this data just below, with encode_utf8()
                    let spare = unsafe {
                        std::mem::transmute::<&mut [std::mem::MaybeUninit<u8>], &mut [u8]>(spare)
                    };
                    c.encode_utf8(&mut spare[..len]);
                    unsafe {
                        output.set_len(pos + len);
                    }
                }
            }
        }
    }

    pub fn tags_to_vec(tags: &Tags, output: &mut Vec<u8>) {
        let mut iter = tags.iter();
        // First item is special, so that we don't need to remove "," at the end
        if let Some((k, v)) = iter.next() {
            write!(output, "\"").unwrap();
            Self::escape_key_value(k, output);
            write!(output, "\"=>\"").unwrap();
            Self::escape_key_value(v, output);
            write!(output, "\"").unwrap();
        }
        for (k, v) in iter {
            write!(output, ",\"").unwrap();
            Self::escape_key_value(k, output);
            write!(output, "\"=>\"").unwrap();
            Self::escape_key_value(v, output);
            write!(output, "\"").unwrap();
        }
    }

    pub fn object_to_line_buffer(
        &mut self,
        id: i64,
        version: i32,
        user_id: i32,
        timestamp: OffsetDateTime,
        changeset_id: i64,
        tags: &Tags,
    ) {
        itoap::write_to_vec(&mut self.line_buffer, id);
        write!(self.line_buffer, "\t").unwrap();
        itoap::write_to_vec(&mut self.line_buffer, version);
        write!(self.line_buffer, "\t").unwrap();
        itoap::write_to_vec(&mut self.line_buffer, user_id);
        write!(self.line_buffer, "\t").unwrap();
        timestamp
            .format_into(&mut self.line_buffer, &self.time_format)
            .unwrap();
        write!(self.line_buffer, "\t").unwrap();
        itoap::write_to_vec(&mut self.line_buffer, changeset_id);
        write!(self.line_buffer, "\t").unwrap();
        Self::tags_to_vec(tags, &mut self.line_buffer);
    }
}

impl OsmWriter for Postgres {
    fn write_node(&mut self, node: &Node) -> Result<(), io::Error> {
        let id = node.id.0;
        let info = node.info.as_ref().unwrap();
        let version = info.version.unwrap();
        let user_id = info.uid.unwrap();
        let timestamp = OffsetDateTime::from_unix_timestamp(info.timestamp.unwrap()).unwrap();
        let changeset_id: i64 = info.changeset.unwrap();
        let tags = &node.tags;

        let lon = node.lon();
        let lat = node.lat();
        let coord: Coord = (lon, lat).into();
        self.nodes.insert(node.id.0, coord);

        self.line_buffer.clear();
        self.object_to_line_buffer(id, version, user_id, timestamp, changeset_id, tags);
        write!(self.line_buffer, "\t").unwrap();
        Self::lonlat_to_ewkb(lon, lat, &mut self.line_buffer);
        writeln!(self.line_buffer).unwrap();

        self.copy.nodes.write_all(&self.line_buffer).unwrap();

        self.users
            .entry(user_id)
            .or_insert_with(|| info.user.as_ref().unwrap().clone());

        Ok(())
    }
    fn write_way(&mut self, way: &Way) -> Result<(), io::Error> {
        let id = way.id.0;
        let info = way.info.as_ref().unwrap();
        let version = info.version.unwrap();
        let user_id = info.uid.unwrap();
        let timestamp = OffsetDateTime::from_unix_timestamp(info.timestamp.unwrap()).unwrap();
        let changeset_id: i64 = info.changeset.unwrap();
        let tags = &way.tags;

        let mut nodes: Vec<i64> = Vec::with_capacity(way.nodes.len());
        let mut nodes_list: Vec<Coord> = Vec::with_capacity(way.nodes.len());
        for n in &way.nodes {
            nodes.push(n.0);
            if let Some(coord) = self.nodes.get(&n.0) {
                nodes_list.push(*coord);
            }
        }
        if (nodes.first() == nodes.last()) && (nodes_list.first() != nodes_list.last()) {
            // Close an originally closed way, but where the first node is missing from extract
            eprintln!("Closing way {}", id);
            nodes_list.push(*nodes_list.first().unwrap())
        }

        self.line_buffer.clear();
        self.object_to_line_buffer(id, version, user_id, timestamp, changeset_id, tags);
        write!(self.line_buffer, "\t").unwrap();
        Self::ids_to_vec(&nodes, &mut self.line_buffer);
        write!(self.line_buffer, "\t").unwrap();
        Self::way_to_ewkb(nodes_list, &mut self.line_buffer);
        writeln!(self.line_buffer).unwrap();

        self.copy.ways.write_all(&self.line_buffer).unwrap();

        self.line_buffer.clear();
        for (i, node) in nodes.iter().enumerate() {
            itoap::write_to_vec(&mut self.line_buffer, id);
            write!(self.line_buffer, "\t").unwrap();
            itoap::write_to_vec(&mut self.line_buffer, *node);
            write!(self.line_buffer, "\t").unwrap();
            itoap::write_to_vec(&mut self.line_buffer, i);
            writeln!(self.line_buffer).unwrap();
        }
        self.copy.way_nodes.write_all(&self.line_buffer).unwrap();

        self.users
            .entry(user_id)
            .or_insert_with(|| info.user.as_ref().unwrap().clone());

        Ok(())
    }
    fn write_relation(&mut self, relation: &Relation) -> Result<(), io::Error> {
        let id = relation.id.0;
        let info = relation.info.as_ref().unwrap();
        let version = info.version.unwrap();
        let user_id = info.uid.unwrap();
        let timestamp = OffsetDateTime::from_unix_timestamp(info.timestamp.unwrap()).unwrap();
        let changeset_id: i64 = info.changeset.unwrap();
        let tags = &relation.tags;

        self.line_buffer.clear();
        self.object_to_line_buffer(id, version, user_id, timestamp, changeset_id, tags);
        writeln!(self.line_buffer).unwrap();

        self.copy.relations.write_all(&self.line_buffer).unwrap();

        for (i, elem) in relation.refs.iter().enumerate() {
            let (member_id, member_type) = match elem.member {
                osmpbfreader::OsmId::Node(id) => (id.0, "N"),
                osmpbfreader::OsmId::Way(id) => (id.0, "W"),
                osmpbfreader::OsmId::Relation(id) => (id.0, "R"),
            };
            let member_role: String = elem.role.to_string();

            writeln!(
                self.copy.relation_members,
                "{id}\t{member_id}\t{member_type}\t{member_role}\t{i}"
            )
            .unwrap();
        }

        self.users
            .entry(user_id)
            .or_insert_with(|| info.user.as_ref().unwrap().clone());

        Ok(())
    }
    fn write_end(&mut self, _change: bool) -> Result<(), Box<dyn Error>> {
        for (uid, name) in self.users.iter() {
            writeln!(self.copy.users, "{uid}\t{}", Self::escape_string(name)).unwrap();
        }
        Ok(())
    }
}
