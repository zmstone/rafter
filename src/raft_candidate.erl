-module(raft_candidate).

-export([ become/2
        , handle_msg/2
        ]).

-export_type([ candidate/0
             ]).

-include("gen_raft_private.hrl").

-define(candidate, ?MODULE).

-record(?candidate,
        { election_timeout    :: timer:time()
        , election_timer      :: ?undef | timer_ref()
        , received_votes = [] :: raft_peers()
        }).

-opaque candidate() :: #?candidate{}.

become(InitArgs, #?state{ raft_meta = RaftMeta
                        , raft_logs = RaftLogs
                        } = State) ->
  ElectionTimeout = getarg(election_timeout, InitArgs,
                           ?DEFAULT_ELECTION_TIMEOUT),
  {ok, NewRaftMeta} = raft_meta:bump_term(RaftMeta),
  %% send to all peers including myself, %% TODO, maybe skip self?
  Peers = raft_meta:get_all_members(NewRaftMeta),
  LastTick = raft_logs:get_lastTick(RaftLogs),
  Request = raft_meta:make_requestVoteRPC(NewRaftMeta, LastTick),
  ok = raft_utils:multi_cast(Peers, Request),
  TimerRef = raft_utils:maybe_start_election_timer(RaftMeta, ElectionTimeout),
  RaftState =
    #?candidate{ election_timeout = ElectionTimeout
               , election_timer   = TimerRef
               },
  gen_raft:loop(State#?state{raft_state = RaftState}).

handle_msg(?raft_requestVoteRPC(_, _, _) = RPC, State) ->
  {ok, NewState} = raft_utils:handle_requestVoteRPC(RPC, State),
  gen_raft:loop(NewState);
handle_msg(?raft_requestVoteReply(FromPeer, VoteGranted, PeerTerm),
           #?state{ name      = Name
                  , raft_meta = RaftMeta
                  } = State) ->
  case raft_meta:get_currentTerm(RaftMeta) of
    MyCurrentTerm when MyCurrentTerm =:= PeerTerm ->
      Result = case VoteGranted of
                  true  -> "granted";
                  false -> "denied"
               end,
      ?info("[~p:term=~w]: vote ~s by ~w\n",
            [Name, MyCurrentTerm, Result, FromPeer]),
      handle_requestVoteReply(FromPeer, VoteGranted, State);
    MyCurrentTerm when MyCurrentTerm > PeerTerm ->
      ?info("[~p:term=~w]: discarded stale requestVoteReply "
            "from=~p, result=~p, peer-term=~p\n",
            [Name, MyCurrentTerm, FromPeer, VoteGranted, PeerTerm]),
       gen_raft:loop(State);
    MyCurrentTerm when MyCurrentTerm < PeerTerm ->
      ?info("[~p:term=~w]: higher term received from ~p, peer-term=~w\n",
            [Name, MyCurrentTerm, FromPeer, PeerTerm]),
      NewRaftMeta = raft_meta:update_currentTerm(RaftMeta, PeerTerm),
      gen_raft:loop(State#?state{raft_meta = NewRaftMeta})
  end.

%%%*_/ internal functions ======================================================

getarg(Name, Args, Default) ->
  proplists:get_value(Name, Args, Default).

-spec handle_requestVoteReply(raft_peer(), boolean(), #?state{}) -> no_return().
handle_requestVoteReply(FromPeer, _VoteGramted = true,
                        #?state{ raft_meta  = RaftMeta
                               , raft_state = Candidate0
                               } = State) ->
  #?candidate{received_votes = ReceivedVotes0} = Candidate0,
  ReceivedVotes = ordsets:add_element(FromPeer, ReceivedVotes0),
  Candidate = Candidate0#?candidate{received_votes = ReceivedVotes},
  NewState = State#?state{raft_state = Candidate},
  case is_quorum(ReceivedVotes, RaftMeta) of
    true  -> become_leader(NewState);
    false -> gen_raft:loop(NewState)
  end;
handle_requestVoteReply(_FromPeer, _VoteGranted = false, State) ->
  gen_raft:loop(State).

-spec is_quorum(raft_peers(), raft_meta()) -> boolean().
is_quorum(ReceivedVotes, RaftMeta) ->
  ClusterSize = ordsets:size(raft_meta:get_all_members(RaftMeta)),
  VoteCount = ordsets:size(ReceivedVotes),
  VoteCount > (ClusterSize div 2).

-spec cancel_election_timer(#?state{}) -> #?state{}.
cancel_election_timer(#?state{raft_state = Candidate} = State) ->
  #?candidate{election_timer = TimerRef} = Candidate,
  ok = raft_utils:cancel_election_timer(TimerRef),
  NewCandidate = Candidate#?candidate{election_timer = ?undef},
  State#?state{raft_state = NewCandidate}.

cleanup(State) ->
  NewState = cancel_election_timer(State),
  NewState#?state{raft_state = ?undef}.

become_leader(#?state{} = State) ->
  NewState = cleanup(State),
  LeaderInitArgs = [],
  raft_leader:become(LeaderInitArgs, NewState).

