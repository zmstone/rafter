-module(raft_logs).

-export([ get_lastTick/1
        , init/1
        ]).

-export_type([logs/0]).

-include("gen_raft_private.hrl").

-record(logs,
        { lastTick     :: raft_tick()
        , entries = [] :: [{raft_tick(), any()}]
        }).

-opaque logs() :: #logs{}.

%%%*_/ APIs ====================================================================

-spec init(raft_meta()) -> {ok, logs()}.
init(RaftMeta) ->
  Logs = #logs{lastTick = raft_meta:get_lastApplied(RaftMeta)},
  {ok, Logs}.

-spec get_lastTick(logs()) -> raft_tick().
get_lastTick(#logs{lastTick = LastTick}) -> LastTick.

%%%*_/ internal functions ======================================================

