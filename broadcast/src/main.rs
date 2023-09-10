//! # Broadcast
//!
//! The third Gossip Glomers challenge.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::{collections::HashSet, sync::Arc};

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
    fn broadcast(&mut self, framework: Arc<Framework>, msg: Message) {
        let message = msg
            .body
            .get("message")
            .and_then(Value::as_u64)
            .expect("`message` is required and must be a u64");
        if self.seen.insert(message) {
            for neighbor in &self.neighbors {
                let framework = Arc::clone(&framework);
                let mut body = Map::new();
                body.insert("type".to_owned(), "broadcast".into());
                body.insert("message".to_owned(), message.into());
                framework.send_with_retries(neighbor.to_owned(), body);
            }
        }
        let mut body = Map::new();
        body.insert("type".to_owned(), "broadcast_ok".into());
        framework.reply(msg, body)
    }

    /// Reply with seen message IDs.
    fn read(&mut self, framework: Arc<Framework>, msg: Message) {
        let mut body = Map::new();
        body.insert("type".to_owned(), "read_ok".into());
        body.insert("messages".to_owned(), self.seen.iter().copied().collect());
        framework.reply(msg, body)
    }

    /// Set the topology of our neighbors.
    fn topology(&mut self, framework: Arc<Framework>, msg: Message) {
        self.neighbors = msg
            .body
            .get("topology")
            .expect("topology will exist")
            .as_object()
            .expect("topology will be an object")
            .get(&framework.node_id())
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
    fn handle(&mut self, framework: Arc<Framework>, msg: Message) {
        match msg.r#type() {
            "broadcast" => self.broadcast(framework, msg),
            "read" => self.read(framework, msg),
            "topology" => self.topology(framework, msg),
            _ => {}
        }
    }
}

fn main() {
    let node = BroadcastNode::new();
    Framework::run(node)
}
