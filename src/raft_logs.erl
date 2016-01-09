-module(raft_logs).

-export([ get_lastTick/1
        , new/1
        ]).

-export_type([logs/0]).

-include("gen_raft_private.hrl").

-record(logs,
        { lastTick     :: ?undef | raft_tick()
        , entries = [] :: [{raft_tick(), any()}]
        }).

-opaque logs() :: #logs{}.

%%%*_/ APIs ====================================================================

-spec new(raft_tick()) -> {ok, logs()}.
new(LastTick) ->
  Logs = #logs{lastTick = LastTick},
  {ok, Logs}.

-spec get_lastTick(logs()) -> raft_tick().
get_lastTick(#logs{lastTick = LastTick}) -> LastTick.

%%%*_/ internal functions ======================================================

