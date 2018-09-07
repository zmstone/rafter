%% @doc Raft replication log.
%% A data structure for
%% - `raft_cl': committed log entries on disk
%% - `raft_lq' entries in RAM waiting to be committed

-module(raft_rlog).

-export([open/2, close/1]).
-export([cfg_keys/0, get_last_lid/1, get_last_committed_lid/1]).

-export_type([ rlog/0
             , cfg_key/0
             , cfg/0
             ]).

-include("raft_int.hrl").

-type lid() :: raft:lid().
-type my_cfg_key() :: 'TODO'.
-type my_cfg() :: #{my_cfg_key() => term()}.
-type cfg_key() :: my_cfg_key() | raft_cl:cfg_key().
-type cfg() :: #{cfg_key() => term()}.
-type dir() :: string().
-opaque rlog() :: #{ cfg := my_cfg()
                   , committed := raft_cl:cl()
                   , dirty := raft_lq:lq()
                   }.

%% @doc Config keys for rlog.
-spec cfg_keys() -> [raft_cl:cfg_key()].
cfg_keys() -> raft_cl:cfg_keys() ++ my_cfg_keys().

%% @doc Open.
-spec open(dir(), cfg()) -> rlog().
open(Dir, Cfg0) ->
  ClCfg = maps:with(raft_cl:cfg_keys(), Cfg0),
  MyCfg = maps:with(my_cfg_keys(), Cfg0),
  Committed = raft_cl:open(Dir, ClCfg),
  ?LID(_LastEpoch, LastIndex) = raft_cl:get_last_lid(Committed),
  #{ cfg => MyCfg
   , committed => Committed
   , dirty => raft_lq:new(LastIndex + 1)
   }.

%% @doc Close log file fd:s etc.
-spec close(rlog()) -> ok.
close(#{committed := Cl}) -> raft_cl:close(Cl).

%% @doc Return the id of last log entry.
-spec get_last_lid(rlog()) -> lid().
get_last_lid(#{dirty := Dirty} = Rlog) ->
  case raft_lq:get_last_lid(Dirty) of
    empty -> get_last_committed_lid(Rlog);
    Lid -> Lid
  end.

%% @doc Return the id of last committed log entry.
-spec get_last_committed_lid(rlog()) -> lid().
get_last_committed_lid(#{committed := Committed}) ->
  raft_cl:get_last_lid(Committed).

%%%*_/ internal functions ======================================================

%% TODO
my_cfg_keys() -> [].

