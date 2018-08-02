-module(raft_roles).
-behaviour(gen_statem).

-export([start/1, start_link/1, shutdown/1]).
-export([terminate/3, code_change/4, init/1, callback_mode/0]).
-export([loading/3, follower/3, candidate/3, leader/3]).

-export_type([changing_member/0, data/0]).

-include("raft_int.hrl").
-include("raft_cfg.hrl").
-include("raft_msgs.hrl").

%% states
-define(loading, loading).
-define(follower, follower).
-define(candidate, candidate).
-define(leader, leader).

-define(not_initialized, not_initialized).
-define(none, none).
-define(add, add).
-define(del, del).

-define(VERY_FIRST_EPOCH, 0).

-define(DBG(D, Fmt, Args), ?log_debug("~s (~p): " ++ Fmt, [fmt_id(D), maps:get(current_epoch, D) | Args])).
-define(INFO(D, Fmt, Args), ?log_info("~s (~p): " ++ Fmt, [fmt_id(D), maps:get(current_epoch, D) | Args])).

-type epoch() :: raft:epoch().
-type member_id() :: raft:member_id().
-type changing_member() :: ?none | {?add, member_id()} | {?del, member_id()}.
-type cfg() :: map().
-type data() :: #{ changing_member := changing_member()
                 , current_epoch := ?not_initialized | epoch()
                 , leader_id := member_id()
                 , my_id := member_id()
                 , peers := raft_peers:peers()
                 , rlog_pid := pid()
                 , stable_members := [member_id()]
                 , voted_for := ?none
                 , votes := [member_id()]
                 }.

%% internal actions
-define(load_raft_state, load_raft_state).

%%%*_/ APIs ====================================================================

%% @doc Start gen_statem without likning to the pid.
start(Cfg) ->
  gen_statem:start(?MODULE, Cfg, []).

%% @doc Start gen_statem for raft roles.
start_link(Cfg) ->
  gen_statem:start_link(?MODULE, Cfg, []).

shutdown(Pid) ->
  gen_statem:stop(Pid, normal, infinity).

%%%*_/ gen_setatem callbacks ===================================================

callback_mode() -> [state_functions, state_enter].

init(Cfg) ->
  process_flag(trap_exit, true),
  PeerConnModule = maps:get(?peer_conn_module, Cfg),
  MyId = raft_peers:unify_id(PeerConnModule, maps:get(?my_id, Cfg)),
  PeerConnOpts = maps:get(?peer_conn_opts, Cfg, []),
  Peers = raft_peers:init(PeerConnModule, MyId, PeerConnOpts),
  {ok, Pid} = raft_rlog:start_link(data_dir(Cfg), Cfg),
  Base = maps:get(?election_timeout_base, Cfg, ?ELECTION_TIMEOUT_BASE),
  Rand = maps:get(?election_timeout_rand, Cfg, ?ELECTION_TIMEOUT_RAND),
  Opts = #{ election_timeout => {Base, Rand}
          , data_dir => data_dir(Cfg)
          },
  Data = #{ changing_member => ?none
          , current_epoch => ?not_initialized
          , leader_id => ?none
          , my_id => MyId
          , opts => Opts
          , peers => Peers
          , rlog_pid => Pid
          , stable_members => get_initial_members(PeerConnModule, Cfg)
          , voted_for => ?none
          , votes => []
          },
  Action = {next_event, internal, {?load_raft_state, Cfg}},
  {ok, ?loading, Data, Action}.

terminate(Reason, State, #{rlog_pid := RlogPid} = Data) ->
  is_normal(Reason) orelse
    ?INFO(Data, "Terminate at state ~p\nreason: ~p", [State, Reason]),
  raft_rlog:shutdown(RlogPid),
  ok.

code_change(_OldVsn, State, Data, _Extra) ->
  {ok, State, Data}.

%%%*_/ state functions =========================================================

%% An ephemeral state before entering 'follower' state.
%% This is to avoid performing too much work in init function.
loading(enter, _OldState, Data) ->
  {keep_state, Data};
loading(internal, {?load_raft_state, Cfg},
        #{rlog_pid := RlogPid} = Data0) ->
  Data1 = load_raft_state(Data0, RlogPid, Cfg),
  Data = spawn_connectors(Data1),
  next_state(?follower, Data).
%% Intended: do not call common/4 to handle default event for ?loading state.

follower(enter, _OldState, Data) ->
  %% emit an event to self after a delay
  Timeout = get_election_timeout(Data),
  Action = {state_timeout, Timeout, Timeout},
  {keep_state, Data, Action};
follower(state_timeout, Timeout, #{leader_id := ?none} = Data) ->
  ?DBG(Data, "Election timer expired after ~p ms", [Timeout]),
  case is_majority_present(Data) of
    true ->
      next_state(?candidate, Data);
    false ->
      ?DBG(Data, "not enough members present, stay down", []),
      %% repeat_state instead of keep_state so the 'enter' event
      %% will be triggered again to have a new timer started.
      {repeat_state, Data}
  end;
follower(state_timeout, Timeout, #{leader_id := Leader} = Data) ->
  ?DBG(Data, "Discarded follower state timeout (after ~p ms)\n"
             "because leader=~p is present", [Timeout, Leader]),
  {keep_state, Data};
follower(info, ?vote_request(Id, Epoch, Lid), Data0) ->
  Data = maybe_grant_vote(Id, Epoch, Lid, Data0),
  {keep_state, Data};
follower(info, ?leader_announcement(Id, _Epoch), Data) ->
  {keep_state, Data#{leader_id := Id}};
follower(Type, Event, Data) ->
  common(?follower, Type, Event, Data).

candidate(enter, _OldState, Data0) ->
  Data = bump_epoch_and_vote_to_self(Data0),
  ok = send_vote_request_to_peers(Data),
  Timeout = get_election_timeout(Data),
  Action = {state_timeout, Timeout, Timeout},
  {keep_state, Data, Action};
candidate(info, ?vote_granted(Id, Epoch), #{current_epoch := Epoch} = Data0) ->
  ?DBG(Data0, "Received vote from ~p", [Id]),
  Data = vote_granted(Data0, Id),
  case has_majority_vote(Data) of
    true -> next_state(?leader, Data);
    false -> {keep_state, Data}
  end;
candidate(info, ?vote_granted(Id, Epoch), Data) ->
  ?DBG(Data, "Stale vote from ~p at epoch = ~p discarded", [Id, Epoch]),
  {keep_state, Data};
candidate(state_timeout, Timeout, Data) ->
  ?DBG(Data, "Failed to elect a leader in ~p ms, try next epoch", [Timeout]),
  {repeat_state, Data};
candidate(info, ?vote_request(Id, Epoch, _Lid), Data) ->
  #{current_epoch := MyEpoch} = Data,
  case Epoch > MyEpoch of
    true ->
      ?DBG(Data, "Higher epoch found from ~p (~p), stepping down", [Id, Epoch]),
      {next_state, Data, postpone};
    false ->
      ?DBG(Data, "Discarded vote request from ~p (~p)", [Id, Epoch]),
      {keep_state, Data}
  end;
candidate(info, ?leader_announcement(Id, _Epoch), Data) ->
  next_state(?follower, Data#{leader_id := Id});
candidate(Type, Event, Data) ->
  common(?candidate, Type, Event, Data).

leader(enter, OldState, #{ current_epoch := Epoch
                         , my_id := MyId
                         } = Data) ->
  ?candidate = OldState, %% assert
  Msg = ?leader_announcement(MyId, Epoch),
  ok = broadcast(Data, Msg),
  {keep_state, Data#{leader_id := MyId}};
leader(info, ?vote_request(Id, Epoch, _Lid),
       #{current_epoch := MyEpoch} = Data) ->
  case Epoch > MyEpoch of
    true ->
      ?DBG(Data, "Higher epoch found from ~p (~p), stepping down", [Id, Epoch]),
      next_state(?follower, Data, postpone);
    false ->
      ?DBG(Data, "Discarded vote request from ~p", [Id]),
      {keep_state, Data}
  end;
leader(Type, Event, Data) ->
  common(?leader, Type, Event, Data).


common(_StateName, info, ?peer_connected(Id, SendFun),
       #{peers := Peers0} = Data0) ->
  Peers = raft_peers:peer_connected(Peers0, Id, SendFun),
  Data = Data0#{peers := Peers},
  {keep_state, Data};
common(_StateName, info, ?peer_down(PeerId),
       #{peers := Peers0} = Data0) ->
  Peers = raft_peers:peer_down(Peers0, PeerId),
  Data = Data0#{peers := Peers},
  {keep_state, Data};
common(StateName, Type, Event, Data) ->
  ?INFO(Data, "Event {~p, ~p} discarded at ~p state", [Type, Event, StateName]),
  {keep_state, Data}.

data_dir(Cfg) -> maps:get(?data_dir, Cfg).

-spec load_raft_state(data(), pid(), cfg()) -> data().
load_raft_state(#{stable_members := Members0} = Data, RlogPid, Cfg) ->
  StateDir = filename:join([data_dir(Cfg), "states"]),
  ok = filelib:ensure_dir(filename:join(StateDir, "foo")),
  case raft_rlog:get_last_lid(RlogPid) of
    false ->
      ?INFO(Data, "No commit log found in ~s", [data_dir(Cfg)]),
      %% no commit history, this is the initial state.
      ok = raft_roles_store:ensure_deleted(StateDir, all),
      Data#{current_epoch := ?VERY_FIRST_EPOCH};
    ?LID(Epoch, _) ->
      ?INFO(Data, "Last epoch found in commit logs ~p", [Epoch]),
      ok = raft_roles_store:ensure_deleted(StateDir, {except, Epoch}),
      #{ voted_for := VotedFor
       , stable_members := Members
       , changing_member := ChangingMember
       } = raft_roles_store:read(StateDir, Epoch),
      %% raft members may change dynamically
      %% the initial members in config file might have been
      %% outdated, use the members in raft state
      Members0 =:= Members orelse
        ?INFO(Data, "Members loaded from raft store: ~p", [Members]),
      Data#{ current_epoch := Epoch
           , voted_for := VotedFor
           , stable_members := Members
           , changing_member := ChangingMember
           }
  end.

get_initial_members(Module, Cfg) ->
  L = maps:get(?initial_members, Cfg),
  lists:usort([raft_peers:unify_id(Module, I) || I <- L]).

all_peer_ids(#{ stable_members := StableMembers
              , changing_member := ChangingMember
              , my_id := MyId
              }) ->
  Peers = StableMembers -- [MyId],
  case ChangingMember of
    ?none -> Peers;
    {?add, Id} -> lists:usort([Id | Peers]);
    {?del, Id} -> lists:usort([Id | Peers])
  end.

spawn_connectors(#{my_id := MyId, peers := Peers0} = Data) ->
  PeerIds = all_peer_ids(Data),
  Peers = raft_peers:spawn_connectors(MyId, Peers0, PeerIds, self()),
  Data#{peers := Peers}.

is_normal(normal) -> true;
is_normal(shutdown) -> true;
is_normal({shutdown, _}) -> true;
is_normal(_) -> false.

get_election_timeout(#{opts := Opts}) ->
  #{election_timeout := {Base, Rand}} = Opts,
  Base + rand:uniform(Rand + 1) - 1.

is_majority_present(#{peers := Peers}) ->
  raft_peers:is_majority_present(Peers).

fmt_id(#{my_id := Id}) -> io_lib:format("~p", [Id]).

bump_epoch_and_vote_to_self(#{ my_id := MyId
                             , current_epoch := Epoch
                             } = Data0) ->
  NewEpoch = Epoch + 1,
  ?DBG(Data0, "Bumpped epoch to ~p", [NewEpoch]),
  self() ! ?vote_granted(MyId, NewEpoch),
  Data = Data0#{ current_epoch := NewEpoch
               , voted_for := MyId
               , votes := []
               },
  ok = persist_role_state(Data),
  Data.

send_vote_request_to_peers(#{ my_id := MyId
                            , current_epoch := Epoch
                            } = Data) ->
  Lid = get_last_lid(Data),
  Msg = ?vote_request(MyId, Epoch, Lid),
  ok = broadcast(Data, Msg).

has_majority_vote(#{votes := Votes} = Data) ->
  length(Votes) > length(all_peer_ids(Data)).

persist_role_state(#{opts := Opts} = Data) ->
  ok = raft_roles_store:write(data_dir(Opts), Data).

next_state(State, Data) ->
  next_state(State, Data, []).

next_state(State, Data, Actions) ->
  ?DBG(Data, "Entering state ~p", [State]),
  {next_state, State, Data, Actions}.

broadcast(#{peers := Peers}, Msg) ->
  ok = raft_peers:broadcast(Peers, Msg).

cast(#{peers := Peers}, Id, Msg) ->
  ok = raft_peers:cast(Peers, Id, Msg).

vote_granted(#{votes := Votes0} = Data, Id) ->
  Votes = lists:usort([Id | Votes0]),
  Data#{votes := Votes}.

is_from_valid_peer(Id, Data) ->
  All = all_peer_ids(Data),
  lists:member(Id, All).

get_last_lid(Pid) when is_pid(Pid) ->
  raft_rlog:get_last_lid(Pid);
get_last_lid(#{rlog_pid := Pid}) ->
  get_last_lid(Pid).

maybe_grant_vote(Id, Epoch, Lid, Data) ->
  case is_from_valid_peer(Id, Data) of
    true ->
      maybe_grant_vote_1(Id, Epoch, Lid, Data);
    false ->
      ?INFO(Data, "Discarded vote request from invalid peer ~p", [Id]),
      Data
  end.

maybe_grant_vote_1(Id, Epoch, Lid, #{current_epoch := MyEpoch} = Data0) ->
  case Epoch < MyEpoch of
    true ->
      ?DBG(Data0, "Discarded vote request from ~p\n"
           "because epoch ~p < my-epoch ~p", [Id, Epoch, MyEpoch]),
      Data0;
    false ->
      Data = maybe_update_epoch(Epoch, Data0),
      maybe_grant_vote_2(Id, Lid, Data)
  end.

maybe_grant_vote_2(Id, _Lid, #{voted_for := VotedFor} = Data) when VotedFor =/= ?none ->
  ?DBG(Data, "Discarded vote request from ~p\n"
       "because I have already voted for ~p", [Id, VotedFor]),
  Data;
maybe_grant_vote_2(Id, Lid, #{my_id := MyId, current_epoch := Epoch} = Data0) ->
  MyLid = get_last_lid(Data0),
  case raft_rlog:is_up_to_date(MyLid, Lid) of
    true ->
      Data = Data0#{voted_for := Id},
      %% persist role state before replying vote request
      %% otherwise there is a risk of double voting in the same epoch
      %% if I crash and restart
      ok = persist_role_state(Data),
      ok = cast(Data, Id, ?vote_granted(MyId, Epoch)),
      ?DBG(Data, "Granted vote to ~p", [Id]),
      Data;
    false ->
      ?DBG(Data0, "Discard vote request from ~p\n"
           "because replication log is not up-to-date lid=~p, my-lid=~p",
           [Id, Lid, MyLid]),
      Data0
  end.

maybe_update_epoch(Epoch, #{current_epoch := MyEpoch} = Data) ->
  case Epoch > MyEpoch of
    true ->
      NewData = Data#{ current_epoch := Epoch
                     , voted_for := ?none
                     },
      ok = persist_role_state(NewData),
      NewData;
    false ->
      Data
  end.
