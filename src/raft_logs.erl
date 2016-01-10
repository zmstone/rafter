-module(raft_logs).

-export([ get_commitTick/1
        , get_lastTick/1
        , new/1
        ]).

-export_type([logs/0]).

-include("gen_raft_private.hrl").

-record(logs,
        { lastTick     :: raft_tick()
        , commitTick   :: raft_tick()
        , entries = [] :: [raft_log()]
        }).

-opaque logs() :: #logs{}.

%%%*_/ APIs ====================================================================

-spec new(raft_tick()) -> {ok, logs()}.
new(?undef) ->
  new(?raft_tick(_Term = 0, _Index = 0));
new(LastTick) ->
  Logs = #logs{ lastTick   = LastTick
              , commitTick = LastTick
              },
  {ok, Logs}.

-spec get_lastTick(logs()) -> raft_tick().
get_lastTick(#logs{lastTick = LastTick}) -> LastTick.

-spec get_commitTick(logs()) -> raft_tick().
get_commitTick(#logs{commitTick = CommitTick}) -> CommitTick.

%%%*_/ internal functions ======================================================

