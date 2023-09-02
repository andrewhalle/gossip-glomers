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

/// A message handler than we can register by calling [`rpc()`].
type Callback = dyn Fn(Message) -> io::Result<()>;

/// A user-defined node must at a minimum implement [`handle()`].
pub trait Node {
    /// Handle a message.
    fn handle(&mut self, framework: &mut Framework, msg: Message) -> io::Result<()>;

    /// Initialize a node. We provide a default empty implementation because some initialization
    /// is handled by the framework and most [`Node`]s won't need this.
    fn init(&mut self, _msg: &Message) -> io::Result<()> {
        Ok(())
    }
}

/// Implements runtime utilities that [`Node`]s can rely on.
pub struct Framework {
    /// The id of this node. Initialized when we receive the `init` message.
    id: Option<String>,

    /// The next message id this node will produce.
    next_msg_id: u64,

    /// A locked handle to the stdout owned by this node.
    stdout: StdoutLock<'static>,

    /// Callback functions registered by sending a message with [`rpc()`].
    callbacks: HashMap<u64, Box<Callback>>,
}

impl Framework {
    /// Send a message to a node without expecting a response.
    pub fn send(&mut self, to: String, body: Map<String, Value>) -> io::Result<()> {
        let msg = Message {
            src: self.node_id().to_owned(),
            dest: to,
            body,
        };
        self.stdout.write_all(
            serde_json::to_string(&msg)
                .expect("message will serialize")
                .as_bytes(),
        )?;
        self.stdout.write_all(b"\n")?;
        Ok(())
    }

    /// Send a message to a node expecting a response, registering a callback to be run when we
    /// receive a response.
    pub fn rpc(
        &mut self,
        to: String,
        mut body: Map<String, Value>,
        callback: Box<Callback>,
    ) -> io::Result<()> {
        let msg_id = self.produce_msg_id();
        body.insert("msg_id".to_owned(), msg_id.into());
        self.send(to, body)?;
        self.callbacks.insert(msg_id, callback);
        Ok(())
    }

    /// Get the current NodeId
    ///
    /// # Panics
    ///     - If the node ID has not already been set.
    pub fn node_id(&mut self) -> &str {
        self.id.as_deref().unwrap()
    }

    /// Reply to a message.
    pub fn reply(&mut self, msg: Message, mut body: Map<String, Value>) -> io::Result<()> {
        if let Some(msg_id) = msg.msg_id() {
            body.insert("in_reply_to".to_owned(), msg_id.into());
        }
        body.insert("msg_id".to_owned(), self.produce_msg_id().into());
        self.send(msg.src, body)
    }

    /// Create a new framework.
    fn new() -> Framework {
        Framework {
            id: None,
            next_msg_id: 1,
            stdout: io::stdout().lock(),
            callbacks: HashMap::default(),
        }
    }

    /// Run the node, accepting and producing messages.
    pub fn run(mut node: impl Node) -> Result<(), io::Error> {
        let mut framework = Framework::new();
        let stdin = io::stdin().lock();
        for msg in stdin.lines() {
            let msg: Message =
                serde_json::from_str(&msg?).expect("we know we won't get invalid data");
            match (msg.r#type(), msg.in_reply_to()) {
                ("init", _) => {
                    node.init(&msg)?;
                    framework.init(msg)?;
                }
                (_, Some(msg_id)) => {
                    if let Some(handler) = framework.callbacks.remove(&msg_id) {
                        handler(msg)?;
                    }
                }
                _ => node.handle(&mut framework, msg)?,
            }
        }
        Ok(())
    }

    /// Handle the `init` message.
    fn init(&mut self, msg: Message) -> io::Result<()> {
        self.id = msg
            .body
            .get("node_id")
            .and_then(|value| value.as_str())
            .map(|s| s.to_owned());
        let mut body = Map::new();
        body.insert("type".to_string(), "init_ok".into());
        self.reply(msg, body)
    }

    /// Make a new `msg_id` unique to this process.
    fn produce_msg_id(&mut self) -> u64 {
        if self.next_msg_id == u64::MAX {
            panic!("too many ids generated");
        }
        let id = self.next_msg_id;
        self.next_msg_id += 1;
        id
    }
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
    pub fn r#type(&self) -> &str {
        self.body
            .get("type")
            .expect("`type` is required")
            .as_str()
            .expect("`type` is always string")
    }

    /// Return the `msg_id` field (optional).
    fn msg_id(&self) -> Option<u64> {
        self.body
            .get("msg_id")
            .map(|value| value.as_u64().expect("`msg_id` is always u64"))
    }

    /// Return the `in_reply_to` field (optional).
    fn in_reply_to(&self) -> Option<u64> {
        self.body
            .get("in_reply_to")
            .map(|value| value.as_u64().expect("`in_reply_to` is always u64"))
    }
}
