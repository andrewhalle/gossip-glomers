//! # Maelstrom
//!
//! Implements the maelstrom protocol for the Gossip Glomer challenges.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::{
    collections::HashMap,
    io::{self, BufRead, StdoutLock, Write},
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A function which can be registered to respond to a message with [`handle()`].
type Handler = dyn FnMut(&mut Node, Message) -> Result<(), io::Error>;

/// Implementors of distributed algorithms.
pub struct Node {
    /// The id of this node. Initialized when we receive the `init` message.
    id: Option<String>,

    /// The next message id this node will produce.
    next_msg_id: u64,

    /// The handlers that have been registered for this node.
    handlers: HashMap<String, Box<Handler>>,

    /// A locked handle to the stdout owned by this node.
    stdout: StdoutLock<'static>,
}

/// A message from another [`Node`].
#[derive(Debug, Deserialize, Serialize)]
pub struct Message {
    /// The NodeId of the sender.
    src: String,

    /// The NodeId of the receiver.
    dest: String,

    /// The payload of the message.
    pub body: Map<String, Value>,
}

impl Message {
    /// Return the `type` field (nonoptional).
    fn r#type(&self) -> &str {
        self.body
            .get("type")
            .expect("`type` is required")
            .as_str()
            .expect("`type` is always string")
    }

    /// Return the `msg_id` field (nonoptional).
    fn msg_id(&self) -> u64 {
        self.body
            .get("msg_id")
            .expect("`msg_id` is required")
            .as_u64()
            .expect("`msg_id` is always u64")
    }
}

impl Default for Node {
    fn default() -> Node {
        Node::new()
    }
}

impl Node {
    /// Create a new node.
    pub fn new() -> Node {
        let mut node = Node {
            handlers: HashMap::new(),
            stdout: io::stdout().lock(),
            id: None,
            next_msg_id: 1,
        };
        node.handle(
            "init",
            Box::new(|node, msg| {
                node.id = msg
                    .body
                    .get("node_id")
                    .and_then(|value| value.as_str())
                    .map(|s| s.to_owned());
                let mut body = Map::new();
                body.insert("type".to_string(), "init_ok".into());
                node.reply(msg, body)
            }),
        );
        node
    }

    /// Register a handler function.
    pub fn handle(&mut self, message_name: impl Into<String>, handler: Box<Handler>) -> &mut Node {
        self.handlers.insert(message_name.into(), handler);
        self
    }

    /// Run the node, accepting and producing messages.
    pub fn run(&mut self) -> Result<(), io::Error> {
        let stdin = io::stdin().lock();
        for msg in stdin.lines() {
            let msg: Message =
                serde_json::from_str(&msg?).expect("we know we won't get invalid data");
            if let Some((r#type, mut handler)) = self.handlers.remove_entry(msg.r#type()) {
                handler(self, msg)?;
                self.handlers.insert(r#type, handler);
            }
        }
        Ok(())
    }

    /// Reply to a message.
    pub fn reply(&mut self, msg: Message, mut body: Map<String, Value>) -> Result<(), io::Error> {
        dbg!(&body);
        body.insert("in_reply_to".to_owned(), msg.msg_id().into());
        body.insert("msg_id".to_owned(), self.produce_msg_id().into());
        let msg = Message {
            src: msg.dest,
            dest: msg.src,
            body,
        };
        self.stdout.write_all(
            serde_json::to_string(&msg)
                .expect("message will serialize")
                .as_bytes(),
        )?;
        self.stdout.write_all(b"\n")
    }

    /// Make a new `msg_id` unique to this node.
    fn produce_msg_id(&mut self) -> u64 {
        if self.next_msg_id == u64::MAX {
            panic!("too many ids generated");
        }
        let id = self.next_msg_id;
        self.next_msg_id += 1;
        id
    }
}
