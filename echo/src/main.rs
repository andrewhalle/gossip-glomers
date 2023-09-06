//! # Echo
//!
//! The first Gossip Glomers challenge.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::io;

use maelstrom::{Message, Node, NodeImpl};

/// Return the message unchanged.
fn echo(node: &mut dyn Node<()>, msg: Message) -> Result<(), io::Error> {
    let mut body = msg.body.clone();
    body.insert("type".to_owned(), "echo_ok".into());
    node.reply(msg, body)
}

fn main() {
    let mut node = NodeImpl::new();
    node.handle("echo", Box::new(echo));
    node.run().unwrap();
}
