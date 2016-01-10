-module(raft_leader).

-export([ become/2
        , handle_msg/3
        ]).

-export_type([ leader/0
             ]).

-include("gen_raft_private.hrl").

-define(leader, ?MODULE).

-record(?leader,
        {
        }).

-opaque leader() :: #?leader{}.

become(_InitArgs, #?state{ cb_mod   = CbMod
                         , cb_state = CbState
                         } = State) ->
  Leader = #?leader{},
  ok = notify_peers(State),
  {ok, NewCbState} = CbMod:elected(CbState),
  gen_raft:continue(State#?state{ cb_state   = NewCbState
                                , raft_state = Leader
                                }).

handle_msg(_From, _Msg, State) ->
  gen_raft:continue(State).

%%%*_/ internal functions ======================================================

notify_peers(#?state{raft_meta = RaftMeta}) ->
  Peers = raft_meta:get_peer_members(RaftMeta),
  lists:foreach(
    fun(_Peer) ->
      %raft_util:cast(Peer, ?)
      ok
    end, Peers).

