//! # Unique ID Generator
//!
//! The second Gossip Glomers challenge.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use maelstrom::Node;
use serde_json::Map;
use uuid::Uuid;

fn main() {
    let mut node = Node::new();
    node.handle(
        "generate",
        Box::new(|node, msg| {
            let mut body = Map::new();
            body.insert("type".to_string(), "generate_ok".into());
            body.insert("id".to_string(), Uuid::new_v4().to_string().into());
            node.reply(msg, body)
        }),
    );
    node.run().unwrap();
}
