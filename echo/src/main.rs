//! # Echo
//!
//! The first Gossip Glomers challenge.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::io;

use maelstrom::{Framework, Message, Node};

/// Echos messages unchanged.
struct EchoNode;
impl EchoNode {
    /// Echo the message back unchanged.
    fn echo(&mut self, framework: &mut Framework, msg: Message) -> Result<(), io::Error> {
        let mut body = msg.body.clone();
        body.insert("type".to_owned(), "echo_ok".into());
        framework.reply(msg, body)
    }
}

impl Node for EchoNode {
    fn handle(&mut self, framework: &mut Framework, msg: Message) -> io::Result<()> {
        match msg.r#type() {
            "echo" => self.echo(framework, msg),
            _ => Ok(()),
        }
    }
}

fn main() {
    Framework::run(EchoNode).unwrap();
}
