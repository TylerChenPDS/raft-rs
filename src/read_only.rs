// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;

use eraftpb::Message;

use hashbrown::{HashMap, HashSet};

/// Determines the relative safety of and consistency of read only requests.
///
/// 更高效的处理只读请求。
/// 如果没有额外的预防措施，绕过日志可能会导致只读查询的结果陈旧。
/// 例如，一个leader可能从集群的其他部分中分离出来，集群的其他部分可能已经选举了一个新的leader，并将新的条目提交到Raft日志中。
/// 如果分区leader响应一个只读查询而不咨询其他服务器，它将返回陈旧的结果
///
/// 这些结果是不可线性化的。线性化要求读取的结果反映读取开始后某个时间的系统状态; 每次读操作必须至少返回最近一次提交的写操作的结果
/// (允许陈旧读取的系统只能提供可序列化性，这是一种较弱的一致性形式。)
///
/// 算法步骤（来自论文）
/// 1. 如果leader还没有将其当前任期的条目标记为已提交，那么它将等待直到提交完成。Leader完整性属性保证Leader拥有所有已提交的条目，但在其任期开始时，
/// 它可能不知道哪些条目是提交的。
/// 为了找到答案，它需要从它的任期中提交一个条目。Raft通过让每个leader在其任期开始时在日志中提交一个空白的无操作（no-op）条目来处理这个问题。
///
/// 2. leader将当前的提交索引保存在本地变量readIndex中。这将用作查询操作所针对的状态版本的下限。
///
/// 3. 领导者需要确保自己没有被一个新的领导者所取代，而它没有意识到。它会发出一轮新的心跳，并等待来自集群大部分成员的确认。
///
/// 4. leader等待它的状态机至少推进到readIndex。
///
/// 5. 最后，leader对其状态机发出查询并返回结果给客户端
///
/// follower还可以帮助减轻对只读查询的处理。这将提高系统的读吞吐量，也将负载从leader转移，允许leader处理更多的读写请求。
/// 为了安全读取，follower可以向leader发出一个请求，请求当前的readIndex (leader会执行上面的步骤1-3);
/// 然后，跟踪者可以在其自己的状态机上为任意数量的累积只读查询执行步骤4和步骤5。
///
/// 通过依赖时钟避免发送过多（心跳）消息。但是不建议使用这种方法，除非遇到了性能需求。
/// 为了在只读查询中使用时钟而不是消息，正常的心跳机制将提供租约的形式。
/// 一旦leader的心跳被集群的大多数成员确认，leader就会假设在选举超时期间没有其他服务器成为leader，并相应地延长其租期。
/// 然后leader在这段时间内回复只读查询，而不需要任何额外的通信。
/// 领导转移机制允许领导提前被替换;领导人在移交领导人之前，需要先终止租约。
///
/// 但是上面的方法依赖时钟，如果某个节点时钟走的不精确。可能依然会返回陈旧的数据。
///
/// 一个简单的扩展可以改善提供给客户端的保证。
/// 客户端将跟踪与他们看到的结果相对应的最新索引，并在每次请求时向服务器提供这些信息。
/// 如果服务器收到客户端的请求，而客户端看到的索引大于服务器上一个应用的日志索引，那么服务器将不会为该请求提供服务(暂时)。
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ReadOnlyOption {
    /// Safe guarantees the linearizability of the read only request by
    /// communicating with the quorum. It is the default and suggested option.
    Safe,
    /// LeaseBased ensures linearizability of the read only request by
    /// relying on the leader lease. It can be affected by clock drift.
    /// If the clock drift is unbounded, leader might keep the lease longer than it
    /// should (clock can move backward/pause without any bound). ReadIndex is not safe
    /// in that case.
    LeaseBased,
}

impl Default for ReadOnlyOption {
    fn default() -> ReadOnlyOption {
        ReadOnlyOption::Safe
    }
}

/// ReadState provides state for read only query.
/// It's caller's responsibility to send MsgReadIndex first before getting
/// this state from ready. It's also caller's duty to differentiate if this
/// state is what it requests through request_ctx, e.g. given a unique id as
/// request_ctx.
#[derive(Default, Debug, PartialEq, Clone)]
pub struct ReadState {
    /// The index of the read state.
    pub index: u64,
    /// A datagram consisting of context about the request.
    pub request_ctx: Vec<u8>,
}

#[derive(Default, Debug, Clone)]
pub struct ReadIndexStatus {
    pub req: Message,
    pub index: u64,
    pub acks: HashSet<u64>,
}

#[derive(Default, Debug, Clone)]
pub struct ReadOnly {
    pub option: ReadOnlyOption,
    pub pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
    pub read_index_queue: VecDeque<Vec<u8>>,
}

impl ReadOnly {
    pub fn new(option: ReadOnlyOption) -> ReadOnly {
        ReadOnly {
            option,
            pending_read_index: HashMap::default(),
            read_index_queue: VecDeque::new(),
        }
    }

    /// Adds a read only request into readonly struct.
    ///
    /// `index` is the commit index of the raft state machine when it received
    /// the read only request.
    ///
    /// `m` is the original read only request message from the local or remote node.
    pub fn add_request(&mut self, index: u64, m: Message) {
        let ctx = {
            let key = m.get_entries()[0].get_data();
            if self.pending_read_index.contains_key(key) {
                return;
            }
            key.to_vec()
        };
        let status = ReadIndexStatus {
            req: m,
            index,
            acks: HashSet::default(),
        };
        self.pending_read_index.insert(ctx.clone(), status);
        self.read_index_queue.push_back(ctx);
    }

    /// Notifies the ReadOnly struct that the raft state machine received
    /// an acknowledgment of the heartbeat that attached with the read only request
    /// context.
    pub fn recv_ack(&mut self, m: &Message) -> HashSet<u64> {
        match self.pending_read_index.get_mut(m.get_context()) {
            None => Default::default(),
            Some(rs) => {
                rs.acks.insert(m.get_from());
                // add one to include an ack from local node
                let mut set_with_self = HashSet::default();
                set_with_self.insert(m.get_to());
                rs.acks.union(&set_with_self).cloned().collect()
            }
        }
    }

    /// Advances the read only request queue kept by the ReadOnly struct.
    /// It dequeues the requests until it finds the read only request that has
    /// the same context as the given `m`.
    pub fn advance(&mut self, m: &Message) -> Vec<ReadIndexStatus> {
        let mut rss = vec![];
        if let Some(i) = self.read_index_queue.iter().position(|x| {
            if !self.pending_read_index.contains_key(x) {
                panic!("cannot find correspond read state from pending map");
            }
            *x == m.get_context()
        }) {
            for _ in 0..=i {
                let rs = self.read_index_queue.pop_front().unwrap();
                let status = self.pending_read_index.remove(&rs).unwrap();
                rss.push(status);
            }
        }
        rss
    }

    /// Returns the context of the last pending read only request in ReadOnly struct.
    pub fn last_pending_request_ctx(&self) -> Option<Vec<u8>> {
        self.read_index_queue.back().cloned()
    }

    #[inline]
    pub fn pending_read_count(&self) -> usize {
        self.read_index_queue.len()
    }
}
