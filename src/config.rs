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

// Copyright 2015 The etcd Authors
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

pub use super::read_only::{ReadOnlyOption, ReadState};
use super::{
    errors::{Error, Result},
    INVALID_ID,
};

/// Config contains the parameters to start a raft.
///
/// 与raft算法相关的配置参数都包装在该结构体中。
pub struct Config {
    /// The identity of the local raft. It cannot be 0, and must be unique in the group.
    ///
    /// 本地raft的标识，不能是0，但是必须在整个集群中是唯一的
    pub id: u64,

    /// The IDs of all nodes (including self) in
    /// the raft cluster. It should only be set when starting a new
    /// raft cluster.
    /// Restarting raft from previous configuration will panic if
    /// peers is set.
    /// peer is private and only used for testing right now.
    ///
    /// 集群中所有节点的IDs。当开启一个raft集群的时候它才应该被设置。使用之前的配置文件重启并且peers被设置的话将会panic
    pub peers: Vec<u64>,

    /// The IDs of all learner nodes (maybe include self if
    /// the local node is a learner) in the raft cluster.
    /// learners only receives entries from the leader node. It does not vote
    /// or promote itself.
    ///
    /// 集群中所有learner节点的IDs（如果当前节点是learner的话，那么会包括自己）。
    /// learner 只接受来自leader的entries。它不参与投票和推举自己（成为candidate）
    pub learners: Vec<u64>,

    /// The number of node.tick invocations that must pass between
    /// elections. That is, if a follower does not receive any message from the
    /// leader of current term before ElectionTick has elapsed, it will become
    /// candidate and start an election. election_tick must be greater than
    /// HeartbeatTick. We suggest election_tick = 10 * HeartbeatTick to avoid
    /// unnecessary leader switching
    ///
    /// 如果一个follower在ElectionTick超时之后没有收到当前任期leader的任何消息，那么他将成为candidate并且开始选举。
    /// election_tick必须比HeartbeatTick要大。建议election_tick = 10 * HeartbeatTick以避免不必要的leader切换
    pub election_tick: usize,

    /// HeartbeatTick is the number of node.tick invocations that must pass between
    /// heartbeats. That is, a leader sends heartbeat messages to maintain its
    /// leadership every heartbeat ticks.
    ///
    /// 每经过heartbeat tickleader将会发送heartbeat 消息维持它的领导权
    pub heartbeat_tick: usize,

    /// Applied is the last applied index. It should only be set when restarting
    /// raft. raft will not return entries to the application smaller or equal to Applied.
    /// If Applied is unset when restarting, raft might return previous applied entries.
    /// This is a very application dependent configuration.
    ///
    /// applied 就是论文所说的last applied index。 仅仅在重启raft的时候菜应该被设置。
    /// raft不会向更小或等于Applied的应用程序返回条目。如果在重启时未设置Applied, raft可能会返回以前应用的条目。
    /// 这是一个非常依赖于应用程序的配置。
    pub applied: u64,

    /// Limit the max size of each append message. Smaller value lowers
    /// the raft recovery cost(initial probing and message lost during normal operation).
    /// On the other side, it might affect the throughput during normal replication.
    /// Note: math.MaxUusize64 for unlimited, 0 for at most one entry per message.
    ///
    ///限制每个append messag消息的最大大小。较小的值降低了raft复成本(初始探测和正常操作期间消息丢失)。
    /// 另一方面，它可能会影响正常复制期间的吞吐量。注意:math.MaxUusize64表示无限，0表示每个消息最多一个条目。
    pub max_size_per_msg: u64,

    /// Limit the max number of in-flight append messages during optimistic
    /// replication phase. The application transportation layer usually has its own sending
    /// buffer over TCP/UDP. Set to avoid overflowing that sending buffer.
    /// TODO: feedback to application to limit the proposal rate?
    ///
    /// 在乐观复制阶段限制运行中的附加消息的最大数量。应用程序传输层通常通过TCP/UDP有自己的发送缓冲区。设置为避免发送缓冲区溢出。
    /// 设置为避免发送缓冲区溢出。
    /// TODO: 反馈给应用程序以限制提案率
    pub max_inflight_msgs: usize,

    /// Specify if the leader should check quorum activity. Leader steps down when
    /// quorum is not active for an electionTimeout.
    ///
    /// 表示领导是否应该检查quorum活动。当electionTimeout超时之后，如果法定人数不够，leader就会退位
    pub check_quorum: bool,

    /// Enables the Pre-Vote algorithm described in raft thesis section
    /// 9.6. This prevents disruption when a node that has been partitioned away
    /// rejoins the cluster.
    ///
    /// 开启pre-vote 算法，这排除了一个节点分区后重新加入集群的干扰
    pub pre_vote: bool,

    /// The range of election timeout. In some cases, we hope some nodes has less possibility
    /// to become leader. This configuration ensures that the randomized election_timeout
    /// will always be suit in [min_election_tick, max_election_tick).
    /// If it is 0, then election_tick will be chosen.
    ///
    /// election_timeout 在 [min_election_tick, max_election_tick) 随机选择。如果min_election_tick=0，
    /// 则election_timeout=election_tick
    pub min_election_tick: usize,

    /// If it is 0, then 2 * election_tick will be chosen.
    ///
    /// 如果它是0， max_election_tick=election_tick * 2
    pub max_election_tick: usize,

    /// Choose the linearizability mode or the lease mode to read data.
    /// If you don’t care about the read consistency and want a higher read performance, you can use the lease mode.
    ///
    /// Setting this to `LeaseBased` requires `check_quorum = true`.
    ///
    /// 选择linearizability mode或者lease mode 进行读操作。如果我么你不在意读的一致性，并且想要更高的性能，可以使用lease mode
    /// 详情参见 `ReadOnlyOption`
    pub read_only_option: ReadOnlyOption,

    /// Don't broadcast an empty raft entry to notify follower to commit an entry.
    /// This may make follower wait a longer time to apply an entry. This configuration
    /// May affect proposal forwarding and follower read.
    ///
    /// 不要广播空raft条目以通知跟随者提交条目。这可能会使跟随者等待更长的时间来应用一个条目。此配置可能会影响提案转发和follower 读。
    pub skip_bcast_commit: bool,

    /// A human-friendly tag used for logging.
    pub tag: String,
}

impl Default for Config {
    fn default() -> Self {
        const HEARTBEAT_TICK: usize = 2;
        Self {
            id: 0,
            peers: vec![],
            learners: vec![],
            election_tick: HEARTBEAT_TICK * 10,
            heartbeat_tick: HEARTBEAT_TICK,
            applied: 0,
            max_size_per_msg: 0,
            max_inflight_msgs: 256,
            check_quorum: false,
            pre_vote: false,
            min_election_tick: 0,
            max_election_tick: 0,
            read_only_option: ReadOnlyOption::Safe,
            skip_bcast_commit: false,
            tag: "".into(),
        }
    }
}

impl Config {
    /// Creates a new config.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            tag: format!("{}", id),
            ..Self::default()
        }
    }

    /// The minimum number of ticks before an election.
    #[inline]
    pub fn min_election_tick(&self) -> usize {
        if self.min_election_tick == 0 {
            self.election_tick
        } else {
            self.min_election_tick
        }
    }

    /// The maximum number of ticks before an election.
    #[inline]
    pub fn max_election_tick(&self) -> usize {
        if self.max_election_tick == 0 {
            2 * self.election_tick
        } else {
            self.max_election_tick
        }
    }

    /// Runs validations against the config.
    pub fn validate(&self) -> Result<()> {
        if self.id == INVALID_ID {
            return Err(Error::ConfigInvalid("invalid node id".to_owned()));
        }

        if self.heartbeat_tick == 0 {
            return Err(Error::ConfigInvalid(
                "heartbeat tick must greater than 0".to_owned(),
            ));
        }

        if self.election_tick <= self.heartbeat_tick {
            return Err(Error::ConfigInvalid(
                "election tick must be greater than heartbeat tick".to_owned(),
            ));
        }

        let min_timeout = self.min_election_tick();
        let max_timeout = self.max_election_tick();
        if min_timeout < self.election_tick {
            return Err(Error::ConfigInvalid(format!(
                "min election tick {} must not be less than election_tick {}",
                min_timeout, self.election_tick
            )));
        }

        if min_timeout >= max_timeout {
            return Err(Error::ConfigInvalid(format!(
                "min election tick {} should be less than max election tick {}",
                min_timeout, max_timeout
            )));
        }

        if self.max_inflight_msgs == 0 {
            return Err(Error::ConfigInvalid(
                "max inflight messages must be greater than 0".to_owned(),
            ));
        }

        if self.read_only_option == ReadOnlyOption::LeaseBased && !self.check_quorum {
            return Err(Error::ConfigInvalid(
                "read_only_option == LeaseBased requires check_quorum == true".into(),
            ));
        }

        Ok(())
    }
}
