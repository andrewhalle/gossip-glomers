//! # Echo
//!
//! The first Gossip Glomers challenge.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::sync::Arc;

use maelstrom::{Framework, Message, Node};

/// Echos messages unchanged.
struct EchoNode;
impl EchoNode {
    /// Echo the message back unchanged.
    fn echo(&mut self, framework: &Framework, msg: Message) {
        let mut body = msg.body.clone();
        body.insert("type".to_owned(), "echo_ok".into());
        framework.reply(msg, body)
    }
}

impl Node for EchoNode {
    fn handle(&mut self, framework: Arc<Framework>, msg: Message) {
        if msg.r#type() == "echo" {
            self.echo(&framework, msg);
        }
    }
}

fn main() {
    Framework::run(EchoNode)
}
