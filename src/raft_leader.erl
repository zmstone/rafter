-module(raft_leader).

-export([ become/2
        , handle_msg/3
        ]).

-export_type([ leader/0
             ]).

-include("gen_raft_private.hrl").

-define(leader, ?MODULE).

-type peer_ticks() :: gb_trees:tree(raft_peer(), raft_tick()).

-record(?leader,
        { peer_ticks = gb_trees:empty() :: peer_ticks()
        }).

-opaque leader() :: #?leader{}.

become(_InitArgs, #?state{ cb_mod   = CbMod
                         , cb_state = CbState
                         } = State) ->
  loginfo(State, "becoming leader", []),
  Leader = #?leader{},
  NewState = State#?state{raft_state = Leader},
  ok = notify_peers(NewState),
  {ok, NewCbState} = CbMod:elected(CbState),
  gen_raft:continue(NewState#?state{cb_state = NewCbState}).

handle_msg(From, #requestVoteRPC{newTerm = NewTerm} = RPC,
           #?state{raft_meta = RaftMeta} = State) ->
  MyCurrentTerm = raft_meta:get_currentTerm(RaftMeta),
  case NewTerm > MyCurrentTerm of
    true ->
      log_stepdown(State, From, requestVoteRPC, NewTerm),
      {ok, NewState} = stepdown(State),
      raft_follower:become(NewState, From, RPC);
    false ->
      ok = raft_utils:send_requestVoteReply(From, false, RaftMeta),
      gen_raft:continue(State)
  end;
handle_msg(From, #requestVoteReply{peerTerm = PeerTerm}, State) ->
  ContinueFun = fun() -> gen_raft:continue(State) end,
  maybe_stepdown(State, From, requestVoteReply, PeerTerm, ContinueFun);
handle_msg(From, #appendEntriesReply{ peerTerm = PeerTerm
                                    , success  = IsSuccess
                                    }, State) ->
  ContinueFun = case IsSuccess of
                  true  -> fun() -> gen_raft:continue(State) end;
                  false -> fun() -> install_snapshot(From, State) end
                end,
  maybe_stepdown(State, From, appendEntriesReply, PeerTerm, ContinueFun).

%%%*_/ internal functions ======================================================

install_snapshot(_From, State) ->
  %% TODO
  gen_raft:continue(State).

maybe_stepdown(#?state{raft_meta = RaftMeta} = State,
               From, MessageName, PeerTerm, ContinueFun) ->
  MyCurrentTerm = raft_meta:get_currentTerm(RaftMeta),
  case PeerTerm > MyCurrentTerm of
    true ->
      log_stepdown(State, From, MessageName, PeerTerm),
      NewRaftMeta = raft_meta:update_currentTerm(RaftMeta, PeerTerm),
      {ok, NewState} = stepdown(State#?state{raft_meta = NewRaftMeta}),
      raft_follower:become(NewState);
    false ->
      ContinueFun()
  end.

stepdown(#?state{cb_mod = CbMod, cb_state = CbState} = State) ->
  {ok, NewCbState} = CbMod:stepdown(CbState),
  {ok, State#?state{cb_state = NewCbState}}.

notify_peers(#?state{ raft_meta  = RaftMeta
                    , raft_logs  = RaftLogs
                    , raft_state = Leader
                    }) ->
  #?leader{peer_ticks = PeerTicks} = Leader,
  MyId = raft_meta:get_myId(RaftMeta),
  Peers = raft_meta:get_peer_members(RaftMeta),
  MyCurrentTerm = raft_meta:get_currentTerm(RaftMeta),
  LastTick = raft_logs:get_lastTick(RaftLogs),
  CommitTick = raft_logs:get_commitTick(RaftLogs),
  lists:foreach(
    fun(Peer) ->
      PrevTick = case get_peerTick(Peer, PeerTicks) of
                   ?raft_tick(_, _) = PeerTick -> PeerTick;
                   ?undef                      -> LastTick
                 end,
      Msg = #appendEntriesRPC{ leaderTerm   = MyCurrentTerm
                             , prevTick     = PrevTick
                             , entries      = [] %% heartbeat
                             , leaderCommit = CommitTick
                             },
      raft_utils:cast(Peer, ?raft_msg(MyId, Msg))
    end, Peers).

%% @private Get the last known-sent tick for a specific peer.
-spec get_peerTick(raft_peer(), peer_ticks()) -> raft_tick() | ?undef.
get_peerTick(Peer, PeerTicks) ->
  case gb_trees:lookup(Peer, PeerTicks) of
    {value, Tick} -> Tick;
    none          -> ?undef
  end.

log_stepdown(State, From, MessageName, PeerTerm) ->
  loginfo(State, "higher term (~w) received from ~w in ~p, stepping down.",
          [PeerTerm, From, MessageName]).

loginfo(State, Fmt, Args) -> raft_utils:log(info, State, Fmt, Args).

