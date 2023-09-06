//! # Broadcast
//!
//! The third Gossip Glomers challenge.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::{cell::RefCell, collections::HashSet, rc::Rc};

use maelstrom::Node;
use serde_json::{Map, Value};

fn main() {
    let seen = HashSet<u64> = HashSet::default();
    let mut node = Node::with_state(seen);
    node.handle(
        "broadcast",
        Box::new({
            let seen = Rc::clone(&seen);
            move |node, msg| {
                node.state().insert(
                    msg.body
                        .get("message")
                        .and_then(Value::as_u64)
                        .expect("`message` is required and must be a u64"),
                );
                let mut body = Map::new();
                body.insert("type".to_owned(), "broadcast_ok".into());
                node.reply(msg, body)
            }
        }),
    );
    node.handle(
        "read",
        Box::new({
            let seen = Rc::clone(&seen);
            move |node, msg| {
                let seen = seen.borrow_mut();
                let mut body = Map::new();
                body.insert("type".to_owned(), "read_ok".into());
                body.insert("messages".to_owned(), seen.iter().copied().collect());
                node.reply(msg, body)
            }
        }),
    );
    node.handle(
        "topology",
        Box::new(|node, msg| {
            let mut body = Map::new();
            body.insert("type".to_owned(), "topology_ok".into());
            node.reply(msg, body)
        }),
    );
    node.run().unwrap();
}
