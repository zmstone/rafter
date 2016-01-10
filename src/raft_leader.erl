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
  Leader = #?leader{},
  NewState = State#?state{raft_state = Leader},
  ok = notify_peers(NewState),
  {ok, NewCbState} = CbMod:elected(CbState),
  gen_raft:continue(NewState#?state{cb_state = NewCbState}).

handle_msg(_From, _Msg, State) ->
  gen_raft:continue(State).

%%%*_/ internal functions ======================================================

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

