
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate protobuf;
extern crate raft;
extern crate regex;

use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{str, thread};

use protobuf::Message as PbMessage;
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole};
use regex::Regex;
use std::env;
use std::io::{Error, ErrorKind, Read};
use std::str::FromStr;
use std::num::ParseIntError;
use std::net::TcpListener;

/// 根据启动环境变量，创建节点
/// 第一个参数为nodetype
/// nodetype id leaderip:leaderport ip2:port2 ip3:port // 默认，
/// nodetype 可以为leader 或者 follower
/// 返回值最后一个为client_request_port
fn handle_env_args(args: Vec<String>) -> (Arc<Mutex<Node>>, Arc<Mutex<VecDeque<Proposal>>>, HeartBrokenData) {
    let node_type = &args[1];
    let id = &args[2];

    let leader_addr = Address::from_str(&args[3]).unwrap();
    let follower1_addr = Address::from_str(&args[4]).unwrap();
    let follower2_addr = Address::from_str(&args[5]).unwrap();
    let mut mailboxs: HashMap<u64, Address> = HashMap::new();
    mailboxs.insert(1, leader_addr);
    mailboxs.insert(2, follower1_addr);
    mailboxs.insert(3, follower2_addr);
    let client_request_port = args[6].parse().unwrap();

    let my_id: u64 = id.parse().unwrap();
    let my_port: &Address = mailboxs.get(&my_id).unwrap();
    let addr = format!("0.0.0.0:{}", my_port.1);
    let listener: TcpListener =
        TcpListener::bind(addr).unwrap();

    let (sender, receiver): (Sender<Message>, Receiver<Message>) = mpsc::channel();
    // 创建一个线程，在这一个线程里面处理别节点的Tcp连接
    // 别的节点会通过这个发送Message到当前节点
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Err(e) => {
                    eprintln!("failed: {}", e)
                }
                //在这里将收到的消息转换成Message, 然后发送到管道里面
                Ok(mut stream) => {
                    let mut buf = [0; 102400];
                    let bytes_read = stream.read(&mut buf).unwrap();
                    if bytes_read == 0 {
                        return;
                    }
                    let message = Message::parse_from_bytes(&buf[..bytes_read]).unwrap();
                    sender.send(message);
                }
            }
        }
    });

    let (node, mut heartBrokenData) = if node_type.trim() == "leader" {
        Node::create_raft_leader(id.parse().unwrap(), mailboxs)
    } else {
        Node::create_raft_follower(mailboxs)
    };
    heartBrokenData.client_request_port = client_request_port;
    let data = HeartBrokenData::clone(&heartBrokenData);

    let node_return = Arc::new(Mutex::new(node));
    // 一个全局的待处理提议的队列。新的提议将会加入队列，随后将会被提交到raft集群，提交之后，将会从这个队列里面移除
    let proposals_return = Arc::new(Mutex::new(VecDeque::<Proposal>::new()));
    let proposals = Arc::clone(&proposals_return);
    let mut t = Instant::now();
    let mut node = Arc::clone(&node_return);
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(10));
        loop {
            let mut node = node.lock().unwrap();
            // 节点收到其它节点的消息, 使用循环的原因是因为可能不止一条消息
            match receiver.try_recv() {
                Ok(msg) => node.step(msg),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }

        let mut node = node.lock().unwrap();
        let raft_group = match node.raft_group {
            Some(ref mut r) => r,
            // 这个值为None 说明这个节点还没有被初始化，后面的操作不用做了
            _ => continue,
        };

        if t.elapsed() >= Duration::from_millis(100) {
            // Tick the raft.
            raft_group.tick();
            t = Instant::now();
        }

        // leader 从全局队列里面获取提议，进行处理
        if raft_group.raft.state == StateRole::Leader {
            // Handle new proposals.
            let mut proposals = proposals.lock().unwrap();
            // 对于skip_while，skip_while 使用一个闭包作为参数，对于集合中的每一个元素他都会调用这个闭包，然后忽略这个元素，直到返回false
            // 在false返回之后，对于skip_while的工作就停止了，它返回剩余的元素
            // 那么下面的就是要忽略掉所有已经处理过的消息。然后对剩余消息进行处理
            // 属性proposed不等于0，代表这条消息已经被成功提交到集群上面。
            for p in proposals.iter_mut().skip_while(|p| p.proposed > 0) {
                propose(raft_group, p);
            }
        }

        on_ready(raft_group, Arc::clone(&data.kv_pairs), Arc::clone(&data.mailboxes), &proposals);
    });

    return (node_return, proposals_return, heartBrokenData);
}


fn main() {
    let args: Vec<String> = env::args().collect();
    let (node, proposals, heartBrokenData) = handle_env_args(args);
    env_logger::init();

    // Create 5 mailboxes to send/receive messages. Every node holds a `Receiver` to receive
    // messages from others, and uses the respective `Sender` to send messages to others.
    let (mut tx_vec, mut rx_vec) = (Vec::new(), Vec::new());
    for _ in 0..5 {
        let (tx, rx) = mpsc::channel();
        tx_vec.push(tx);
        rx_vec.push(rx);
    }

    // A global pending proposals queue. New proposals will be pushed back into the queue, and
    // after it's committed by the raft cluster, it will be poped from the queue.
    let proposals = Arc::new(Mutex::new(VecDeque::<Proposal>::new()));

    let mut handles = Vec::new();
    for (i, rx) in rx_vec.into_iter().enumerate() {
        // A map[peer_id -> sender]. In the example we create 5 nodes, with ids in [1, 5].
        let mailboxes = (1..6u64).zip(tx_vec.iter().cloned()).collect();
        let mut node = match i {
            // Peer 1 is the leader.
            0 => Node::create_raft_leader(1, rx, mailboxes),
            // Other peers are followers.
            _ => Node::create_raft_follower(rx, mailboxes),
        };
        let proposals = Arc::clone(&proposals);

        // Tick the raft node per 100ms. So use an `Instant` to trace it.
        let mut t = Instant::now();

        // Here we spawn the node on a new thread and keep a handle so we can join on them later.
        let handle = thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(10));
            loop {
                // Step raft messages.
                match node.my_mailbox.try_recv() {
                    Ok(msg) => node.step(msg),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            let raft_group = match node.raft_group {
                Some(ref mut r) => r,
                // When Node::raft_group is `None` it means the node is not initialized.
                _ => continue,
            };

            if t.elapsed() >= Duration::from_millis(100) {
                // Tick the raft.
                raft_group.tick();
                t = Instant::now();
            }

            // Let the leader pick pending proposals from the global queue.
            if raft_group.raft.state == StateRole::Leader {
                // Handle new proposals.
                let mut proposals = proposals.lock().unwrap();
                for p in proposals.iter_mut().skip_while(|p| p.proposed > 0) {
                    propose(raft_group, p);
                }
            }

            // Handle readies from the raft.
            on_ready(raft_group, &mut node.kv_pairs, &node.mailboxes, &proposals);
        });
        handles.push(handle);
    }

    // Propose some conf changes so that followers can be initialized.
    add_all_followers(proposals.as_ref());

    // Put 100 key-value pairs.
    (0..100u16)
        .filter(|i| {
            let (proposal, rx) = Proposal::normal(*i, "hello, world".to_owned());
            proposals.lock().unwrap().push_back(proposal);
            // After we got a response from `rx`, we can assume the put succeeded and following
            // `get` operations can find the key-value pair.
            rx.recv().unwrap()
        })
        .count();

    for th in handles {
        th.join().unwrap();
    }
}

struct Node {
    raft_group: Option<RawNode<MemStorage>>
}

impl Node {
    // 创建一个只在其配置中包含自身的raft leader。
    // Notice：初始化leader的时候是创建节点RaftNode的，但是创建Follower的时候是不需要创建RaftNode，懒加载？
    fn create_raft_leader(
        id: u64,
        mailboxes: HashMap<u64, Address>,
    ) -> (Self, HeartBrokenData) {
        let mut cfg = example_config();
        cfg.id = id;
        cfg.peers = vec![id];
        cfg.tag = format!("peer_{}", id);

        let mut s = Snapshot::default();
        // 因为我们不想使用相同的配置文件初始化每一个节点，
        // 所以我们使用一个非0的index 强制所有的跟随着首先复制快照，
        // 这会使得所有的节点处于相同的状态
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];
        let storage = MemStorage::new();
        storage.wl().apply_snapshot(s).unwrap();
        let raft_group = Some(RawNode::new(&cfg, storage, vec![]).unwrap());


        (Node {
            raft_group,
        }, HeartBrokenData {
            mailboxes: Arc::new(mailboxes),
            kv_pairs: Arc::new(Mutex::new(HashMap::new())),
            client_request_port: 0
        })
    }

    // Create a raft follower.
    fn create_raft_follower(
        mailboxes: HashMap<u64, Address>
    ) -> (Self, HeartBrokenData) {
        (Node {
            raft_group: None,
        }, HeartBrokenData {
            mailboxes: Arc::new(mailboxes),
            kv_pairs: Arc::new(Mutex::new(HashMap::new())),
            client_request_port: 0
        })
    }

    // Initialize raft for followers.
    fn initialize_raft_from_message(&mut self, msg: &Message) {
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = example_config();
        cfg.id = msg.get_to();
        let storage = MemStorage::new();
        self.raft_group = Some(RawNode::new(&cfg, storage, vec![]).unwrap());
    }

    // Step a raft message, initialize the raft if need.
    fn step(&mut self, msg: Message) {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg);
            } else {
                return;
            }
        }
        let raft_group = self.raft_group.as_mut().unwrap();
        let _ = raft_group.step(msg);
    }
}

fn on_ready(
    raft_group: &mut RawNode<MemStorage>,
    kv_pairs: Arc<Mutex<HashMap<String, String>>>,
    mailboxes: Arc<HashMap<u64, Address>>,
    proposals: &Mutex<VecDeque<Proposal>>,
) {
    if !raft_group.has_ready() {
        return;
    }
    let store = raft_group.raft.raft_log.store.clone();

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = raft_group.ready();

    let handle_messages = |msgs: Vec<Message>| {
        for msg in msgs {
            let to = msg.to;
            if mailboxes[&to].send(msg).is_err() {
                error!(
                    logger,
                    "send raft message to {} fail, let Raft retry it", to
                );
            }
        }
    };

    if !ready.messages().is_empty() {
        // Send out the messages come from the node.
        handle_messages(ready.take_messages());
    }

    // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
    if *ready.snapshot() != Snapshot::default() {
        let s = ready.snapshot().clone();
        if let Err(e) = store.wl().apply_snapshot(s) {
            error!(
                logger,
                "apply snapshot fail: {:?}, need to retry or panic", e
            );
            return;
        }
    }

    let mut handle_committed_entries =
        |rn: &mut RawNode<MemStorage>, committed_entries: Vec<Entry>| {
            for entry in committed_entries {
                if entry.data.is_empty() {
                    // From new elected leaders.
                    continue;
                }
                if let EntryType::EntryConfChange = entry.get_entry_type() {
                    // For conf change messages, make them effective.
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = rn.apply_conf_change(&cc).unwrap();
                    store.wl().set_conf_state(cs);
                } else {
                    // For normal proposals, extract the key-value pair and then
                    // insert them into the kv engine.
                    let data = str::from_utf8(&entry.data).unwrap();
                    let reg = Regex::new("put ([0-9]+) (.+)").unwrap();
                    if let Some(caps) = reg.captures(data) {
                        kv_pairs.insert(caps[1].parse().unwrap(), caps[2].to_string());
                    }
                }
                if rn.raft.state == StateRole::Leader {
                    // The leader should response to the clients, tell them if their proposals
                    // succeeded or not.
                    let proposal = proposals.lock().unwrap().pop_front().unwrap();
                    proposal.propose_success.send(true).unwrap();
                }
            }
        };
    // Apply all committed entries.
    handle_committed_entries(raft_group, ready.take_committed_entries());

    // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
    // raft logs to the latest position.
    if let Err(e) = store.wl().append(ready.entries()) {
        error!(
            logger,
            "persist raft log fail: {:?}, need to retry or panic", e
        );
        return;
    }

    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        store.wl().set_hardstate(hs.clone());
    }

    if !ready.persisted_messages().is_empty() {
        // Send out the persisted messages come from the node.
        handle_messages(ready.take_persisted_messages());
    }

    // Call `RawNode::advance` interface to update position flags in the raft.
    let mut light_rd = raft_group.advance(ready);
    // Update commit index.
    if let Some(commit) = light_rd.commit_index() {
        store.wl().mut_hard_state().set_commit(commit);
    }
    // Send out the messages.
    handle_messages(light_rd.take_messages());
    // Apply all committed entries.
    handle_committed_entries(raft_group, light_rd.take_committed_entries());
    // Advance the apply index.
    raft_group.advance_apply();
}

fn example_config() -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    }
}

// The message can be used to initialize a raft node or not.
fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.get_commit() == 0)
}

struct Proposal {
    normal: Option<(u16, String)>, // key is an u16 integer, and value is a string.
    conf_change: Option<ConfChange>, // conf change.
    transfer_leader: Option<u64>,
    // If it's proposed, it will be set to the index of the entry.
    proposed: u64,
    propose_success: SyncSender<u64>,
}

impl Proposal {
    fn conf_change(cc: &ConfChange) -> (Self, Receiver<u64>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: None,
            conf_change: Some(cc.clone()),
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }

    fn normal(key: u16, value: String) -> (Self, Receiver<u64>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: Some((key, value)),
            conf_change: None,
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }
}

fn propose(raft_group: &mut RawNode<MemStorage>, proposal: &mut Proposal) {
    let last_index1 = raft_group.raft.raft_log.last_index() + 1;
    if let Some((ref key, ref value)) = proposal.normal {
        println!("proposal——normal {}:{}", key, value);
        let data = format!("{},{}", key, value).into_bytes();
        // 普通消息提交到raftNode 处理
        let _ = raft_group.propose(vec![], data);
        // 成员变更消息处理流程
    } else if let Some(ref cc) = proposal.conf_change {
        let _ = raft_group.propose_conf_change(vec![], cc.clone());
    } else if let Some(_tranferee) = proposal.transfer_leader {
        // TODO: implement tranfer leader.
        // unimplemented!();
    }

    let last_index2 = raft_group.raft.raft_log.last_index() + 1;
    // 如果处理成功entry日志会自动增加
    if last_index2 == last_index1 {
        // Propose failed, don't forget to respond to the client.
        // 向客户端发出响应
        proposal.propose_success.send(0).unwrap();
    } else {
        proposal.proposed = last_index1;
    }
}

// Proposes some conf change for peers [2, 5].
fn add_all_followers(proposals: &Mutex<VecDeque<Proposal>>) {
    for i in 2..6u64 {
        let mut conf_change = ConfChange::default();
        conf_change.set_node_id(i);
        conf_change.set_change_type(ConfChangeType::AddNode);
        loop {
            let (proposal, rx) = Proposal::conf_change(&conf_change);
            proposals.lock().unwrap().push_back(proposal);
            if rx.recv().unwrap() {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}



fn is_leader(node: Arc<Mutex<Node>>) -> bool {
    let mut node = node.lock().unwrap();
    let raft_group = match node.raft_group {
        Some(ref mut r) => r,
        _ => return false,
    };
    if raft_group.raft.state == StateRole::Leader {
        return true;
    }
    return false;
}

struct HeartBrokenData {
    // 保存了所有人管道，用于向其他人发送消息
    mailboxes: Arc<HashMap<u64, Address>>,
    //一个KV 数据库
    kv_pairs: Arc<Mutex<HashMap<String, String>>>,
    client_request_port: u64
}

impl Clone for HeartBrokenData {
    fn clone(&self) -> HeartBrokenData {
        HeartBrokenData {
            kv_pairs: Arc::clone(&self.kv_pairs),
            mailboxes: Arc::clone(&self.mailboxes),
            client_request_port: self.client_request_port
        }
    }
}


#[derive(Debug, Default, Eq, Clone)]
pub struct Address(pub String, pub u16);

impl FromStr for Address {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let tokens: Vec<&str> = s.split(":").collect();
        if tokens.len() != 2 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Port input expected: `label:port`, actual: {}", &s),
            ));
        }
        let label = tokens[0];
        let port: Result<u16, ParseIntError> = tokens[1].parse();
        if let Err(_e) = port {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("expected u16 type as port, actual: {:?}", &tokens[1]),
            ));
        }
        Ok(Port(label.to_owned(), port.unwrap()))
    }
}


// fn main() {
//
// }