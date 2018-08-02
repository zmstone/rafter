-module(raft).

-export([start/1, start_link/1, shutdown/1]).
-export([main/1]).
-export_type([lid/0, index/0, epoch/0, wal/0, member_id/0]).

-type index() :: non_neg_integer().
-type epoch() :: non_neg_integer().
-type lid() :: {epoch(), index()}.
-type wal() :: binary().
-type member_id() :: raft_peers:id().

start(Cfg) -> raft_roles:start(Cfg).

start_link(Cfg) -> raft_roles:start_link(Cfg).

shutdown(Pid) -> raft_roles:shutdown(Pid).

main(EscriptArgs) -> raft_es:main(EscriptArgs).

