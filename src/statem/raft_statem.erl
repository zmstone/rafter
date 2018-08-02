-module(raft_statem).

-type statem() :: term().

-callback start(Args :: term()) -> statem().
-callback get_last_lid(statem()) -> raft:lid().
-callback apply_wal(statem(), raft:wal()) -> statem().

