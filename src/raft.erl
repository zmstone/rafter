-module(raft).

-export([start/1, start_link/1, shutdown/1]).
-export([main/1]).
-export_type([lid/0, idx/0, gnr/0, wal/0, member_id/0]).

-type idx() :: integer().
-type gnr() :: integer().
-type lid() :: {gnr(), idx()}.
-type wal() :: binary().
-type member_id() :: raft_peers:id().

start(Cfg) -> raft_roles:start(Cfg).

start_link(Cfg) -> raft_roles:start_link(Cfg).

shutdown(Pid) -> raft_roles:shutdown(Pid).

main(EscriptArgs) -> raft_es:main(EscriptArgs).

