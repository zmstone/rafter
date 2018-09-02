%% @doc Raft replication log.
%% A `gen_server' which manages `raft_cl' for committed log entries on disk,
%% and `raft_lq' for entries in RAM waiting to be committed.

-module(raft_rlog).
-behaviour(gen_server).

-export([start_link/2, cfg_keys/0, get_last_lid/1, shutdown/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).
-export([is_up_to_date/2]).

-include("raft_int.hrl").

-define(not_initialized, not_initialized).

-type lid() :: raft:lid().

start_link(Dir, Cfg) ->
  gen_server:start_link(?MODULE, {Dir, Cfg}, []).

shutdown(Pid) -> gen_server:stop(Pid, normal, infinity).

cfg_keys() -> raft_cl:cfg_keys() ++ [].

-spec get_last_lid(pid()) -> lid().
get_last_lid(Pid) ->
  gen_server:call(Pid, get_last_lid, infinity).

%% @doc Return 'true' if other's last lid is up-to-date comparing to mine.
-spec is_up_to_date(lid(), lid()) -> boolean().
is_up_to_date(MyLid, OthersLid) ->
  MyLid =:= false orelse OthersLid >= MyLid.

%%%*_/ gen_server callbacks ====================================================

init({Dir, Cfg0}) ->
  process_flag(trap_exit, true),
  ClCfgKeys = raft_cl:cfg_keys(),
  ClCfg = maps:with(ClCfgKeys, Cfg0),
  MyCfg = maps:without(ClCfgKeys, Cfg0),
  self() ! {do_init, Dir, ClCfg},
  {ok, #{ cfg => MyCfg
        , cl => ?not_initialized
        , lq => ?not_initialized
        }}.

handle_info({do_init, Dir, ClCfg}, St) ->
  Cl = raft_cl:open(Dir, ClCfg),
  {noreply, St#{cl := Cl}};
handle_info(Info, St) ->
  ?log_debug("unknown info: ~p", [Info]),
  {noreply, St}.

handle_call(get_last_lid, _From, #{cl := Cl} = St) ->
  Res = raft_cl:get_last_lid(Cl),
  {reply, Res, St};
handle_call(Call, _From, St) ->
  ?log_debug("unknown call: ~p", [Call]),
  {noreply, St}.

handle_cast(Cast, St) ->
  ?log_debug("unknown cast: ~p", [Cast]),
  {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
  {ok, St}.

terminate(Reason, #{cl := Cl} = St) ->
  ?log_debug("terminate reason: ~p", [Reason]),
  ok = raft_cl:close(Cl),
  {ok, St}.

%%%*_/ internal functions ======================================================

