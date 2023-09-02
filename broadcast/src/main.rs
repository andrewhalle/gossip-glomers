//! # Broadcast
//!
//! The third Gossip Glomers challenge.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::{collections::HashSet, io};

use maelstrom::{Framework, Message, Node};
use serde_json::{Map, Value};

/// Handles broadcasting.
struct BroadcastNode {
    /// Which message IDs have been seen.
    seen: HashSet<u64>,

    /// The NodeIds of our neighbors.
    neighbors: Vec<String>,
}

impl BroadcastNode {
    /// Create a [`BroadcastNode`].
    fn new() -> BroadcastNode {
        BroadcastNode {
            seen: HashSet::new(),
            neighbors: Vec::new(),
        }
    }

    /// Broadcast a message to all peers.
    fn broadcast(&mut self, framework: &mut Framework, msg: Message) -> io::Result<()> {
        let message = msg
            .body
            .get("message")
            .and_then(Value::as_u64)
            .expect("`message` is required and must be a u64");
        if self.seen.insert(message) {
            let mut body = Map::new();
            body.insert("type".to_owned(), "broadcast_ok".into());
            framework.reply(msg, body)?;
            for neighbor in &self.neighbors {
                let mut body = Map::new();
                body.insert("type".to_owned(), "broadcast".into());
                body.insert("message".to_owned(), message.into());
                framework.send(neighbor.to_owned(), body)?;
            }
        }
        Ok(())
    }

    /// Reply with seen message IDs.
    fn read(&mut self, framework: &mut Framework, msg: Message) -> io::Result<()> {
        let mut body = Map::new();
        body.insert("type".to_owned(), "read_ok".into());
        body.insert("messages".to_owned(), self.seen.iter().copied().collect());
        framework.reply(msg, body)
    }

    /// Set the topology of our neighbors.
    fn topology(&mut self, framework: &mut Framework, msg: Message) -> io::Result<()> {
        self.neighbors = msg
            .body
            .get("topology")
            .expect("topology will exist")
            .as_object()
            .expect("topology will be an object")
            .get(framework.node_id())
            .expect("node_id will exist in topology")
            .as_array()
            .expect("neighbors will be an array")
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .expect("neighbor will be a string")
                    .to_owned()
            })
            .collect();
        let mut body = Map::new();
        body.insert("type".to_owned(), "topology_ok".into());
        framework.reply(msg, body)
    }
}

impl Node for BroadcastNode {
    fn handle(&mut self, framework: &mut Framework, msg: Message) -> io::Result<()> {
        match msg.r#type() {
            "broadcast" => self.broadcast(framework, msg),
            "read" => self.read(framework, msg),
            "topology" => self.topology(framework, msg),
            _ => Ok(()),
        }
    }
}

fn main() {
    let node = BroadcastNode::new();
    Framework::run(node).unwrap();
}
