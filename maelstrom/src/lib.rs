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

/// A function which can be registered to respond to a message with [`handle_with_state()`].
type HandlerWithState<S> = dyn FnMut(&mut dyn Node<S>, Message) -> Result<(), io::Error>;
/// A function which can be registered to respond to a message with [`handle()`].
type Handler = HandlerWithState<()>;

/// The public API for a node.
pub trait Node<S>: private::NodePrivate {
    /// Reply to a given message.
    fn reply(&mut self, msg: Message, body: Map<String, Value>) -> Result<(), io::Error>;

    /// Access the node's state.
    fn state(&mut self) -> Option<&mut S>;
}

/// Module for sealing [`NodePrivate`].
mod private {
    /// Behavior for a node that we can rely on internally to this library.
    pub trait NodePrivate {
        /// Set `id`.
        fn set_id(&mut self, id: Option<String>);
    }
}

/// Implementors of distributed algorithms.
pub struct NodeImpl<S> {
    /// The id of this node. Initialized when we receive the `init` message.
    id: Option<String>,

    /// The next message id this node will produce.
    next_msg_id: u64,

    /// The handlers that have been registered for this node.
    handlers: HashMap<String, Box<Handler>>,

    /// The stateful handlers that have been registered for this node.
    stateful_handlers: HashMap<String, Box<HandlerWithState<S>>>,

    /// A locked handle to the stdout owned by this node.
    stdout: StdoutLock<'static>,

    /// The state available to handler functions.
    state: S,
}

impl<S> private::NodePrivate for NodeImpl<S> {
    fn set_id(&mut self, id: Option<String>) {
        self.id = id;
    }
}

impl<S> Node<S> for NodeImpl<S>
where
    S: NonEmptyState,
{
    fn reply(&mut self, msg: Message, body: Map<String, Value>) -> Result<(), io::Error> {
        <NodeImpl<S> as Node<()>>::reply(self, msg, body)
    }

    fn state(&mut self) -> Option<&mut S> {
        Some(&mut self.state)
    }
}

/// A marker trait that identifies state as not the unit type.
pub trait NonEmptyState {}

impl<S> Node<()> for NodeImpl<S>
where
    S: NonEmptyState,
{
    fn reply(&mut self, msg: Message, mut body: Map<String, Value>) -> Result<(), io::Error> {
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

    fn state(&mut self) -> Option<&mut ()> {
        None
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

impl Default for NodeImpl<()> {
    fn default() -> NodeImpl<()> {
        NodeImpl::new()
    }
}

impl NodeImpl<()> {
    /// Create a new node.
    pub fn new() -> NodeImpl<()> {
        let mut node = NodeImpl {
            handlers: HashMap::new(),
            stateful_handlers: HashMap::new(),
            stdout: io::stdout().lock(),
            id: None,
            next_msg_id: 1,
            state: (),
        };
        node.add_init_handler();
        node
    }
}

impl<S> NodeImpl<S> {
    /// Add the handler for the `init` message.
    fn add_init_handler(&mut self) {
        self.handle(
            "init",
            Box::new(|node, msg| {
                node.set_id(
                    msg.body
                        .get("node_id")
                        .and_then(|value| value.as_str())
                        .map(|s| s.to_owned()),
                );
                let mut body = Map::new();
                body.insert("type".to_string(), "init_ok".into());
                node.reply(msg, body)
            }),
        );
    }

    /// Register a handler function.
    pub fn handle(
        &mut self,
        message_name: impl Into<String>,
        handler: Box<Handler>,
    ) -> &mut NodeImpl<S> {
        self.handlers.insert(message_name.into(), handler);
        self
    }

    /// Run the node, accepting and producing messages.
    pub fn run(mut self) -> Result<(), io::Error> {
        let stdin = io::stdin().lock();
        for msg in stdin.lines() {
            let msg: Message =
                serde_json::from_str(&msg?).expect("we know we won't get invalid data");
            if let Some((r#type, mut handler)) = self.handlers.remove_entry(msg.r#type()) {
                handler(&mut self, msg)?;
                self.handlers.insert(r#type, handler);
            }
        }
        Ok(())
    }
}

impl<S> NodeImpl<S>
where
    S: NonEmptyState,
{
    /// Create a new node with some state available to handler functions.
    pub fn with_state(state: S) -> NodeImpl<S> {
        let NodeImpl {
            handlers,
            stdout,
            id,
            next_msg_id,
            ..
        } = NodeImpl::new();
        NodeImpl {
            handlers,
            stateful_handlers: HashMap::new(),
            stdout,
            id,
            next_msg_id,
            state,
        }
    }

    /// Register a stateful handler.
    pub fn handle_with_state(
        &mut self,
        message_name: impl Into<String>,
        handler: Box<HandlerWithState<S>>,
    ) -> &mut NodeImpl<S> {
        self.stateful_handlers.insert(message_name.into(), handler);
        self
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
