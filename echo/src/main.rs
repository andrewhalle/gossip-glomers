//! # Echo
//!
//! The first Gossip Glomers challenge.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::io;

use maelstrom::{Message, Node};

/// Return the message unchanged.
fn echo(node: &mut Node, msg: Message) -> Result<(), io::Error> {
    let mut body = msg.body.clone();
    body.insert("type".to_owned(), "echo_ok".into());
    node.reply(msg, body)
}

fn main() {
    let mut node = Node::new();
    node.handle("echo", Box::new(echo));
    node.run().unwrap();
}
