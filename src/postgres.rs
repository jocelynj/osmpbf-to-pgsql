//! Access postgres+postgis database

use chrono::DateTime;
use geo_types::{Coord, Geometry, LineString, Point};
use geozero;
use geozero::{CoordDimensions, ToWkb};
use itertools::Itertools;
use osmpbfreader::objects::{Node, Relation, Tags, Way};
use postgres::{Client, NoTls};
use rustc_hash::FxHashMap;
use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};
use std::path::Path;

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
            nodes: FxHashMap::default(),
            users: FxHashMap::default(),
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

    pub fn to_hex_string(bytes: Vec<u8>) -> String {
        let strs: Vec<String> = bytes.iter().map(|b| format!("{:02X}", b)).collect();
        strs.concat()
    }

    pub fn ids_to_string(ids: &[i64]) -> String {
        let mut s: String = "{".to_string();
        s.push_str(&ids.iter().join(","));
        s.push('}');
        s
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

    pub fn escape_key_value(s: &str) -> String {
        // Differs from escape_string() as " is escaped with 2 slashes instead of 0.
        let mut out = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '"' => out.push_str("\\\\\""),
                _ => out.push(c),
            }
        }
        out
    }

    pub fn tags_to_string(tags: &Tags) -> String {
        let strs: Vec<String> = tags
            .iter()
            .map(|(k, v)| {
                format!(
                    "\"{}\"=>\"{}\"",
                    Self::escape_key_value(k),
                    Self::escape_key_value(v)
                )
            })
            .collect();
        strs.join(",")
    }
}

impl OsmWriter for Postgres {
    fn write_node(&mut self, node: &Node) -> Result<(), io::Error> {
        let id = node.id.0;
        let info = node.info.as_ref().unwrap();
        let version = info.version.unwrap();
        let user_id = info.uid.unwrap();
        let timestamp = DateTime::from_timestamp(info.timestamp.unwrap(), 0)
            .unwrap()
            .with_timezone(&chrono::Local)
            .format("%F %T%z");
        let changeset_id: i64 = info.changeset.unwrap();
        let tags = Self::tags_to_string(&node.tags);

        let lon = node.lon();
        let lat = node.lat();
        let coord: Coord = (lon, lat).into();
        self.nodes.insert(node.id.0, coord);

        let point: Point = (lon, lat).into();
        let point: Geometry = point.into();
        let point = point.to_ewkb(CoordDimensions::xy(), Some(4326)).unwrap();
        let point = Self::to_hex_string(point);

        writeln!(
            self.copy.nodes,
            "{id}\t{version}\t{user_id}\t{timestamp}\t{changeset_id}\t{tags}\t{point}"
        )
        .unwrap();

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
        let timestamp = DateTime::from_timestamp(info.timestamp.unwrap(), 0)
            .unwrap()
            .with_timezone(&chrono::Local)
            .format("%F %T%z");
        let changeset_id: i64 = info.changeset.unwrap();
        let tags = Self::tags_to_string(&way.tags);

        let nodes: Vec<i64> = way.nodes.iter().map(|x| x.0).collect();
        let nodes_list: Vec<Coord> = way
            .nodes
            .iter()
            .filter_map(|&x| self.nodes.get(&x.0))
            .copied()
            .collect();
        let linestring = if nodes.len() == nodes_list.len() {
            match nodes_list.len() {
                0 => None,
                1 => {
                    let point: Point = nodes_list[0].into();
                    let point: Geometry = point.into();
                    Some(point)
                }
                _ => {
                    let linestring: Geometry = LineString::new(nodes_list).into();
                    Some(linestring)
                }
            }
        } else {
            None
        };

        let nodes_str = Self::ids_to_string(&nodes);

        let linestring = match linestring {
            None => String::from("\\N"),
            Some(l) => {
                let linestring = l.to_ewkb(CoordDimensions::xy(), Some(4326)).unwrap();
                Self::to_hex_string(linestring)
            }
        };

        writeln!(self.copy.ways, "{id}\t{version}\t{user_id}\t{timestamp}\t{changeset_id}\t{tags}\t{nodes_str}\t{linestring}").unwrap();
        for (i, node) in nodes.iter().enumerate() {
            writeln!(self.copy.way_nodes, "{id}\t{node}\t{i}").unwrap();
        }

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
        let timestamp = DateTime::from_timestamp(info.timestamp.unwrap(), 0)
            .unwrap()
            .with_timezone(&chrono::Local)
            .format("%F %T%z");
        let changeset_id: i64 = info.changeset.unwrap();
        let tags = Self::tags_to_string(&relation.tags);

        writeln!(
            self.copy.relations,
            "{id}\t{version}\t{user_id}\t{timestamp}\t{changeset_id}\t{tags}"
        )
        .unwrap();
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
