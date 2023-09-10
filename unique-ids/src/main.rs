//! # Unique ID Generator
//!
//! The second Gossip Glomers challenge.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::sync::Arc;

use maelstrom::{Framework, Message, Node};
use serde_json::Map;
use uuid::Uuid;

/// The unique ID generator.
struct UniqueIdNode;

impl UniqueIdNode {
    /// Generate a unique ID.
    fn generate(&mut self, framework: &Framework, msg: Message) {
        let mut body = Map::new();
        body.insert("type".to_string(), "generate_ok".into());
        body.insert("id".to_string(), Uuid::new_v4().to_string().into());
        framework.reply(msg, body)
    }
}

impl Node for UniqueIdNode {
    fn handle(&mut self, framework: Arc<Framework>, msg: Message) {
        if msg.r#type() == "generate" {
            self.generate(&framework, msg);
        }
    }
}

fn main() {
    Framework::run(UniqueIdNode)
}
