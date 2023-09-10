//! # Maelstrom
//!
//! Implements the maelstrom protocol for the Gossip Glomer challenges.

#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use crossbeam::channel::{self, Sender};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A message handler than we can register by calling [`rpc()`].
type Callback = dyn Fn(Message) + Send + Sync + 'static;

/// A user-defined node must at a minimum implement [`handle()`].
pub trait Node {
    /// Handle a message.
    fn handle(&mut self, framework: Arc<Framework>, msg: Message);

    /// Initialize a node. We provide a default empty implementation because some initialization
    /// is handled by the framework and most [`Node`]s won't need this.
    fn init(&mut self, _msg: &Message) {}
}

/// Implements runtime utilities that [`Node`]s can rely on.
pub struct Framework {
    /// The id of this node. Initialized when we receive the `init` message.
    id: Mutex<Option<String>>,

    /// The next message id this node will produce.
    next_msg_id: AtomicU64,

    /// The outbox for this node.
    stdout: Sender<Message>,

    /// Callback functions registered by sending a message with [`rpc()`].
    callbacks: Mutex<HashMap<u64, Box<Callback>>>,
}

impl Framework {
    /// Send a message reliably, retrying every 250ms if we don't get a response.
    pub fn send_with_retries(self: Arc<Framework>, to: String, body: Map<String, Value>) {
        thread::spawn({
            let framework = Arc::clone(&self);
            move || loop {
                let received = Arc::new(AtomicBool::new(false));
                framework.rpc(
                    to.clone(),
                    body.clone(),
                    Box::new({
                        let received = Arc::clone(&received);
                        move |_| received.store(true, Ordering::Relaxed)
                    }),
                );
                thread::sleep(Duration::from_millis(250));
                if received.load(Ordering::Relaxed) {
                    break;
                }
            }
        });
    }

    /// Send a message to a node without expecting a response.
    pub fn send(&self, to: String, body: Map<String, Value>) {
        let msg = Message {
            src: self.node_id().to_owned(),
            dest: to,
            body,
        };
        self.stdout.send(msg).unwrap();
    }

    /// Send a message to a node expecting a response, registering a callback to be run when we
    /// receive a response.
    pub fn rpc(&self, to: String, mut body: Map<String, Value>, callback: Box<Callback>) {
        let msg_id = self.produce_msg_id();
        body.insert("msg_id".to_owned(), msg_id.into());
        self.send(to, body);
        self.callbacks.lock().unwrap().insert(msg_id, callback);
    }

    /// Get the current NodeId
    ///
    /// # Panics
    ///     - If the node ID has not already been set.
    pub fn node_id(&self) -> String {
        self.id.lock().unwrap().as_deref().unwrap().to_owned()
    }

    /// Reply to a message.
    pub fn reply(&self, msg: Message, mut body: Map<String, Value>) {
        if let Some(msg_id) = msg.msg_id() {
            body.insert("in_reply_to".to_owned(), msg_id.into());
        }
        body.insert("msg_id".to_owned(), self.produce_msg_id().into());
        self.send(msg.src, body)
    }

    /// Create a new framework.
    fn new(stdout: Sender<Message>) -> Framework {
        Framework {
            id: Mutex::default(),
            next_msg_id: 1.into(),
            callbacks: Mutex::default(),
            stdout,
        }
    }

    /// Run the node, accepting and producing messages.
    pub fn run(mut node: impl Node) {
        let (stdin_tx, stdin_rx) = channel::unbounded();
        let (stdout_tx, stdout_rx) = channel::unbounded();
        let framework = Arc::new(Framework::new(stdout_tx));
        thread::spawn(move || {
            let stdin = io::stdin().lock();
            for msg in stdin.lines() {
                let msg: Message =
                    serde_json::from_str(&msg.unwrap()).expect("we know we won't get invalid data");
                stdin_tx.send(msg).unwrap();
            }
        });
        thread::spawn(move || {
            let mut stdout = io::stdout().lock();
            for msg in stdout_rx {
                stdout
                    .write_all(
                        serde_json::to_string(&msg)
                            .expect("message will serialize")
                            .as_bytes(),
                    )
                    .unwrap();
                stdout.write_all(b"\n").unwrap();
            }
        });
        for msg in stdin_rx {
            match (msg.r#type(), msg.in_reply_to()) {
                ("init", _) => {
                    node.init(&msg);
                    framework.init(msg);
                }
                (_, Some(msg_id)) => {
                    if let Some(handler) = framework.callbacks.lock().unwrap().remove(&msg_id) {
                        handler(msg);
                    }
                }
                _ => node.handle(Arc::clone(&framework), msg),
            }
        }
    }

    /// Handle the `init` message.
    fn init(&self, msg: Message) {
        *self.id.lock().unwrap() = msg
            .body
            .get("node_id")
            .and_then(|value| value.as_str())
            .map(|s| s.to_owned());
        let mut body = Map::new();
        body.insert("type".to_string(), "init_ok".into());
        self.reply(msg, body)
    }

    /// Make a new `msg_id` unique to this process.
    fn produce_msg_id(&self) -> u64 {
        let id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        if id > u64::MAX >> 1 {
            panic!("too many ids generated");
        }
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
