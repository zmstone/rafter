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

-define(same, same).
-define(newer, newer).
-define(older, older).

-define(outdated, outdated).
-define(rlog_ok, rlog_ok).
-define(rlog_mismatch(PrevLid), {rlog_mismatch, PrevLid}).

-define(FMT_ARGS(D, Args), [maps:get(current_epoch, D), fmt_id(D) | Args]).
-define(DBG(D, Fmt, Args), ?log_debug("[~p] ~s: " ++ Fmt, ?FMT_ARGS(D, Args))).
-define(INF(D, Fmt, Args), ?log_info("[~p] ~s: " ++ Fmt, ?FMT_ARGS(D, Args))).
-define(ERR(D, Fmt, Args), ?log_error("[~p] ~s: " ++ Fmt, ?FMT_ARGS(D, Args))).

-type epoch() :: raft:epoch().
-type member_id() :: raft:member_id().
-type changing_member() :: ?none | {?add, member_id()} | {?del, member_id()}.
-type cfg() :: map().
-type lid() :: raft_rlog:lid().
-type data() :: #{ changing_member := changing_member()
                 , current_epoch := ?not_initialized | epoch()
                 , leader_id := member_id()
                 , my_id := member_id()
                 , peers := raft_peers:peers()
                 , rlog_pid := pid()
                 , last_lid := ?not_initialized | lid()
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
  StatyDown = maps:get(?stay_down_timeout, Cfg, ?STAY_DOWN_TIMEOUT),
  Opts = #{ election_timeout => {Base, Rand}
          , data_dir => data_dir(Cfg)
          , stay_down_timeout => StatyDown
          },
  Data = #{ changing_member => ?none
          , current_epoch => ?not_initialized
          , leader_id => ?none
          , my_id => MyId
          , opts => Opts
          , peers => Peers
          , rlog_pid => Pid
          , last_lid => ?not_initialized
          , stable_members => get_initial_members(PeerConnModule, Cfg)
          , voted_for => ?none
          , votes => []
          },
  Action = {next_event, internal, {?load_raft_state, Cfg}},
  {ok, ?loading, Data, Action}.

terminate(Reason, State, #{rlog_pid := RlogPid} = Data) ->
  is_normal(Reason) orelse
    ?INF(Data, "Terminate at state ~p\nreason: ~p", [State, Reason]),
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
%% Intended: do not call common/4 as 'default' for ?loading state.

follower(enter, _OldState, Data0) ->
  %% entering or repeating follower state
  %% always implies a re-election of new leader
  Data = Data0#{leader_id := ?none},
  %% emit an event to self after a delay
  Timeout = get_follower_state_timeout(Data),
  Action = {state_timeout, Timeout, Timeout},
  {keep_state, Data, Action};
follower(state_timeout, Timeout, #{leader_id := ?none} = Data) ->
  ?DBG(Data, "Follower state expired after ~p ms", [Timeout]),
  case is_majority_present(Data) of
    true ->
      next_state(?candidate, Data);
    false ->
      ?ERR(Data, "Not enough members present, stay down", []),
      %% repeat_state instead of keep_state so the 'enter' event
      %% will be triggered again to have a new timer started.
      {repeat_state, Data}
  end;
follower(state_timeout, _Timeout, Data) ->
  %% discard because we have leader present
  {keep_state, Data};
follower(info, ?vote_req(Id, Epoch0, Lid), Data0) ->
  {IsGranted, Data} = maybe_grant_vote(Id, Epoch0, Lid, Data0),
  #{my_id := MyId, current_epoch := Epoch} = Data,
  ok = cast(Data, Id, ?vote_rsp(MyId, Epoch, IsGranted)),
  {keep_state, Data};
follower(info, ?peer_connected(Id, SendFun),
         #{peers := Peers0, leader_id := ?none} = Data0) ->
  HasMajorityConnection0 = is_majority_present(Data0),
  Peers = raft_peers:peer_connected(Peers0, Id, SendFun),
  Data = Data0#{peers := Peers},
  HasMajorityConnection = is_majority_present(Data),
  case {HasMajorityConnection0, HasMajorityConnection} of
    {false, true} ->
      %% re-enter follower state to trigger a new state timer
      {repeat_state, Data};
    _ ->
      {keep_state, Data}
  end;
follower(info, ?peer_connected(Id, SendFun), #{peers := Peers0} = Data0) ->
  Peers = raft_peers:peer_connected(Peers0, Id, SendFun),
  Data = Data0#{peers := Peers},
  {keep_state, Data};
follower(info, ?peer_down(PeerId), #{leader_id := LeaderId} = Data0) ->
  Data = handle_peer_down(PeerId, Data0),
  case PeerId =:= LeaderId of
    true -> {repeat_state, Data#{leader_id := ?none}};
    false -> {keep_state, Data}
  end;
follower(info, ?step_down(Id, Epoch),
         #{leader_id := Id, current_epoch := Epoch} = Data) ->
  {repeat_state, Data};
follower(Type, Event, Data) ->
  common(?follower, Type, Event, Data).

candidate(enter, _OldState, Data0) ->
  Data = bump_epoch_and_vote_to_self(Data0),
  ok = send_vote_request_to_peers(Data),
  Timeout = get_election_timeout(Data),
  Action = {state_timeout, Timeout, Timeout},
  {keep_state, Data, Action};
candidate(info, ?vote_rsp(Id, Epoch, _IsGranted = true),
          #{current_epoch := Epoch} = Data0) ->
  Data = vote_granted(Data0, Id),
  {VoteCount, MemberCount} = count_votes(Data),
  ?DBG(Data0, "Received vote from ~p, ~p/~p.", [Id, VoteCount, MemberCount]),
  case ?IS_MAJORITY(VoteCount, MemberCount) of
    true  -> next_state(?leader, Data);
    false -> {keep_state, Data}
  end;
candidate(state_timeout, Timeout, Data) ->
  ?DBG(Data, "Failed to elect a leader in ~p ms, try next epoch", [Timeout]),
  {repeat_state, Data};
candidate(info, ?peer_down(PeerId), Data0) ->
  Data = handle_peer_down(PeerId, Data0),
  case is_majority_present(Data) of
    true -> {keep_state, Data};
    false -> next_state(?follower, Data)
  end;
candidate(info, ?peer_connected(_, _), Data) ->
  %% handle it in the next state, either follower or leader
  {keep_state, Data, postpone};
candidate(Type, Event, Data) ->
  common(?candidate, Type, Event, Data).

leader(enter, OldState, #{my_id := MyId} = Data) ->
  ?candidate = OldState, %% assert
  ?INF(Data, "Elected!", []),
  Msg = make_rlog_req(Data),
  ok = broadcast(Data, Msg),
  {keep_state, Data#{leader_id := MyId}};
leader(info, ?peer_connected(Id, SendFun),
       #{peers := Peers0} = Data0) ->
  Peers = raft_peers:peer_connected(Peers0, Id, SendFun),
  Data = Data0#{peers := Peers},
  Msg = make_rlog_req(Data),
  ok = cast(Data, Id, Msg),
  {keep_state, Data};
leader(info, ?peer_down(PeerId), Data0) ->
  Data = handle_peer_down(PeerId, Data0),
  case is_majority_present(Data) of
    true -> {keep_state, Data};
    false ->
      ?ERR(Data, "Lost connection to majority, stepping down", []),
      do_step_down(Data, [])
  end;
leader(info, ?vote_rsp(Id, Epoch, _IsGranted = true),
       #{current_epoch := Epoch} = Data0) ->
  Data = vote_granted(Data0, Id),
  {VoteCount, MemberCount} = count_votes(Data),
  ?DBG(Data0, "Received vote from ~p, ~p/~p.", [Id, VoteCount, MemberCount]),
  {keep_state, Data};
leader(info, ?rlog_rsp(Id, Epoch, ?outdated), Data0) ->
  {IsNewEpoch, Data} = maybe_update_epoch(Id, Epoch, Data0),
  IsNewEpoch = true, %% assert
  ?INF(Data, "Higher epoch [~p] from ~p, stepping down", [Epoch, Id]),
  do_step_down(Data, []);
leader(info, ?rlog_rsp(Id, _Epoch, ?rlog_ok), Data) ->
  handle_rlog_ok(Data, Id);
leader(info, ?rlog_rsp(Id, _Epoch, ?rlog_mismatch(PrevLid)), Data) ->
  handle_rlog_mismatch(Data, Id, PrevLid);
leader(Type, Event, Data) ->
  common(?leader, Type, Event, Data).

common(_StateName, info, ?vote_req(Id, Epoch, _Lid), #{my_id := MyId} = Data0) ->
  {IsNewEpoch, Data} = maybe_update_epoch(Id, Epoch, Data0),
  case IsNewEpoch of
    true ->
      ?DBG(Data0, "Higher epoch found from ~p (~p), stepping down", [Id, Epoch]),
      %% no response here, postpone it to follower state
      next_state(?follower, Data, postpone);
    false ->
      ok = cast(Data, Id, ?vote_rsp(MyId, Epoch, _IsGranted = false)),
      ?DBG(Data, "Discarded vote request from ~p", [Id]),
      {keep_state, Data}
  end;
common(_StateName, info, ?vote_rsp(Id, Epoch, _IsGranted), Data0) ->
  {IsNewEpoch, Data} = maybe_update_epoch(Id, Epoch, Data0),
  case IsNewEpoch of
    true ->
      next_state(?follower, Data);
    false ->
      ?DBG(Data, "Discarded vote response from ~p", [Id]),
      {keep_state, Data}
  end;
common(_StateName, info, ?step_down(Id, Epoch), Data) ->
  ?DBG(Data, "At ~p state discarded step_down message from ~p [~p]", [Id, Epoch]),
  {keep_state, Data};
common(StateName, info, ?rlog_req(Id, LeaderEpoch, Args),
       #{current_epoch := Epoch} = Data) ->
  case compare_epoch(LeaderEpoch, Epoch) of
    ?older ->
      %% an outdated leader is trying to replicate log to me,
      %% ignore and tell it to update epoch
      ok = cast_rlog_rsp(Data, Id, ?outdated),
      {keep_state, Data};
    _ ->
      handle_rlog_req(StateName, Data, Id, LeaderEpoch, Args)
  end;
common(StateName, Type, Event, Data) ->
  ?INF(Data, "Event {~p, ~p} discarded at ~p state", [Type, Event, StateName]),
  {keep_state, Data}.

%%%*_/ Internals ===============================================================

do_step_down(#{my_id := MyId, current_epoch := Epoch} = Data, Action) ->
  ok = broadcast(Data, ?step_down(MyId, Epoch)),
  next_state(?follower, Data, Action).

handle_rlog_req(StateName, Data0, Id, Epoch, Args) ->
  {_IsNewEpoch, Data} = maybe_update_epoch(Id, Epoch, Data0),
  handle_rlog_req_1(StateName, Data, Id, Args).

handle_rlog_req_1(?follower, Data0, Id, Args) ->
  Data1 = maybe_update_leader(Id, Data0),
  {Result, Data} = append_rlogs(Data1, Args),
  ok = cast_rlog_rsp(Data, Id, Result),
  {keep_state, Data};
handle_rlog_req_1(?candidate, Data, _Id, _Args) ->
  %% new leader, handle this event at follower state
  next_state(?follower, Data, postpone);
handle_rlog_req_1(?leader, Data, _Id, _Args) ->
  %% new leader, I am outdated
  %% step down and handle this event at follower state
  do_step_down(Data, postpone).

make_rlog_req(#{ my_id := MyId
               , current_epoch := Epoch
               }) ->
  Args = #{ prev_lid => todo
          , commit_index => todo
          },
  ?rlog_req(MyId, Epoch, Args).

handle_rlog_ok(Data, _Id) ->
  %% TODO
  {keep_state, Data}.

handle_rlog_mismatch(Data, _Id, _PrevLid) ->
  %% TODO
  {keep_state, Data}.

append_rlogs(Data, _Args) ->
  %% TODO
  {?rlog_ok, Data}.

maybe_update_leader(Id, #{leader_id := ?none} = Data) ->
  ?INF(Data, "Discovered leader ~p", [Id]),
  Data#{leader_id := Id};
maybe_update_leader(Id, #{leader_id := OldLeader} = Data) ->
  ?INF(Data, "New leader ~p replacing old leader ~p", [Id, OldLeader]),
  Data#{leader_id := Id}.

cast_rlog_rsp(#{ current_epoch := Epoch
               , my_id := Id
               } = Data, PeerId, Result) ->
  cast(Data, PeerId, ?rlog_rsp(Id, Epoch, Result)).

handle_peer_down(PeerId, #{peers := Peers0, leader_id := LeaderId} = Data) ->
  case PeerId =:= LeaderId of
    true -> ?INF(Data, "Leader ~p down!", [PeerId]);
    false -> ?INF(Data, "Peer ~p down!", [PeerId])
  end,
  Peers = raft_peers:peer_down(Peers0, PeerId),
  Data#{peers := Peers}.

data_dir(Cfg) -> maps:get(?data_dir, Cfg).

-spec load_raft_state(data(), pid(), cfg()) -> data().
load_raft_state(#{stable_members := Members0} = Data0, RlogPid, Cfg) ->
  StateDir = filename:join([data_dir(Cfg), "states"]),
  ok = filelib:ensure_dir(filename:join(StateDir, "foo")),
  LastLid = raft_rlog:get_last_lid(RlogPid),
  ?LID(Epoch, _) = LastLid,
  ok = raft_roles_store:ensure_deleted(StateDir, {except, Epoch}),
  Data = Data0#{current_epoch := Epoch, last_lid := LastLid},
  case raft_roles_store:read(StateDir, Epoch) of
    not_found ->
      Data;
    #{ voted_for := VotedFor
     , stable_members := Members
     , changing_member := ChangingMember
     } ->
      %% raft members may change dynamically
      %% the initial members in config file might have been
      %% outdated, use the members in raft state
      Members0 =:= Members orelse
        ?INF(Data, "Members loaded from raft store: ~p", [Members]),
      Data#{ voted_for := VotedFor
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

get_follower_state_timeout(#{opts := Opts} = Data) ->
  case is_majority_present(Data) of
    true -> get_election_timeout(Data);
    false -> maps:get(stay_down_timeout, Opts)
  end.

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
  self() ! ?vote_rsp(MyId, NewEpoch, true),
  Data = Data0#{ current_epoch := NewEpoch
               , voted_for := MyId
               },
  ok = persist_role_state(Data),
  Data.

send_vote_request_to_peers(#{ my_id := MyId
                            , current_epoch := Epoch
                            } = Data) ->
  Lid = get_last_lid(Data),
  Msg = ?vote_req(MyId, Epoch, Lid),
  ok = broadcast(Data, Msg).

count_votes(#{votes := Votes} = Data) ->
  {length(Votes), 1 + length(all_peer_ids(Data))}.

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
      ?INF(Data, "Discarded vote request from invalid peer ~p", [Id]),
      {false, Data}
  end.

maybe_grant_vote_1(Id, Epoch, Lid, #{current_epoch := MyEpoch} = Data0) ->
  case compare_epoch(Epoch, MyEpoch) of
    ?older ->
      ?DBG(Data0, "Reject vote request from ~p\n"
           "because epoch ~p < my-epoch ~p", [Id, Epoch, MyEpoch]),
      {false, Data0};
    ?same ->
      maybe_grant_vote_2(Id, Lid, Data0);
    ?newer ->
      Data = update_epoch(Id, Epoch, Data0),
      maybe_grant_vote_2(Id, Lid, Data)
  end.

maybe_grant_vote_2(Id, _Lid, #{voted_for := VotedFor} = Data) when VotedFor =/= ?none ->
  ?DBG(Data, "Reject vote request from ~p\n"
       "because I have already voted for ~p", [Id, VotedFor]),
  {false, Data};
maybe_grant_vote_2(Id, Lid, Data0) ->
  MyLid = get_last_lid(Data0),
  case raft_rlog:is_up_to_date(MyLid, Lid) of
    true ->
      Data = Data0#{voted_for := Id},
      %% persist role state before replying vote request
      %% otherwise there is a risk of double voting in the same epoch
      %% if I crash and restart
      ok = persist_role_state(Data),
      ?DBG(Data, "Grant vote to ~p", [Id]),
      {true, Data};
    false ->
      ?DBG(Data0, "Reject vote request from ~p\n"
           "because replication log is not up-to-date lid=~p, my-lid=~p",
           [Id, Lid, MyLid]),
      {false, Data0}
  end.

%% Id is only for logging
maybe_update_epoch(Id, Epoch, #{current_epoch := MyEpoch} = Data) ->
  case compare_epoch(Epoch, MyEpoch) of
    ?newer -> {true, update_epoch(Id, Epoch, Data)};
    _ -> {false, Data}
  end.

update_epoch(Id, Epoch, Data) ->
  ?DBG(Data, "Newer epoch [~p] received from ~p", [Epoch, Id]),
  NewData = Data#{ current_epoch := Epoch
                 , voted_for := ?none
                 },
  ok = persist_role_state(NewData),
  NewData.

compare_epoch(Epoch, Epoch) -> ?same;
compare_epoch(Others, Mine) ->
  case Others < Mine of
    true -> ?older;
    false -> ?newer
  end.

