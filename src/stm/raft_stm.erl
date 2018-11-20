-module(raft_stm).
-export([load/2, get_last_lid/2, make_wal/2, apply_wal/4]).
-export_type([cfg/0, stm/0]).

-type cfg() :: term().
-type stm() :: term().
-type lid() :: raft:lid().
-type wal() :: raft:wal().
-type op() :: term().

-callback load(Cfg :: term()) -> stm().
-callback get_last_lid(stm()) -> lid().
-callback apply_wal(stm(), lid(), wal()) -> stm().
-callback make_wal(op()) -> wal().

-include("raft_int.hrl").

%% @doc Called at start/restart.
%% Load persisted state from disk.
-spec load(module(), cfg()) -> stm().
load(Mod, Cfg) -> Mod:load(Cfg).

%% @doc Read last-lid from statemachine.
%% This is for log feeder to know from which point on it
%% should feed logs to the statemachine after start/restart
-spec get_last_lid(module(), stm()) -> lid().
get_last_lid(Mod, Stm) ->
  case Mod:get_last_lid(Stm) of
    ?LID(_, _) = Lid -> Lid;
    X when X =:= false orelse X =:= undefined -> ?NO_PREV_LID
  end.

%% @doc Apply log entry to statemachine.
-spec apply_wal(module(), stm(), lid(), wal()) -> stm().
apply_wal(Mod, Stm, Lid, Wal) ->
  Mod:apply_wal(Stm, Lid, Wal).

%% @doc Serialize statemachine OP into binary log entry.
-spec make_wal(module(), op()) -> wal().
make_wal(Mod, Op) ->
  Mod:make_wal(Op).

