-module(raft_candidate).

-export([ become/1
        , handle_msg/3
        ]).

-export_type([ candidate/0
             ]).

-include("gen_raft_private.hrl").

-define(candidate, ?MODULE).

-record(?candidate,
        { election_timer      :: ?undef | timer_ref()
        , received_votes = [] :: raft_peers()
        }).

-opaque candidate() :: #?candidate{}.

become(#?state{} = State) ->
  loginfo(State, "becoming candidate", []),
  start_new_term_election(State#?state{raft_state = #?candidate{}}).

handle_msg(From, #requestVoteRPC{} = RPC, State) ->
  {ok, NewState} = raft_utils:handle_requestVoteRPC(From, RPC, State),
  gen_raft:continue(NewState);
handle_msg(From, #requestVoteReply{ voteGranted = VoteGranted
                                  , peerTerm    = PeerTerm},
           #?state{raft_meta = RaftMeta} = State) ->
  case raft_meta:get_currentTerm(RaftMeta) of
    MyCurrentTerm when MyCurrentTerm =:= PeerTerm ->
      Result = case VoteGranted of
                  true  -> "granted";
                  false -> "denied"
               end,
      loginfo(State, "vote ~s by ~w", [Result, From]),
      handle_requestVoteReply(From, VoteGranted, State);
    MyCurrentTerm when MyCurrentTerm > PeerTerm ->
      loginfo(State, "discarded stale requestVoteReply "
              "from=~p, result=~p, peer-term=~p",
              [From, VoteGranted, PeerTerm]),
       gen_raft:continue(State);
    MyCurrentTerm when MyCurrentTerm < PeerTerm ->
      loginfo(State, "higher term received from ~p, peer-term=~w",
              [From, PeerTerm]),
      NewRaftMeta = raft_meta:update_currentTerm(RaftMeta, PeerTerm),
      gen_raft:continue(State#?state{raft_meta = NewRaftMeta})
  end;
handle_msg(self, #electionTimeout{ref = MsgRef},
           #?state{raft_state = Candidate0} = State) ->
  #?candidate{election_timer = TimerRef} = Candidate0,
  {MsgRef, _Tref} = TimerRef, %% assert
  Candidate = Candidate0#?candidate{ election_timer = ?undef
                                   , received_votes = []
                                   },
  start_new_term_election(State#?state{raft_state = Candidate});
handle_msg(From, #appendEntriesRPC{leaderTerm = LeaderTerm} = RPC,
           #?state{raft_meta = RaftMeta} = State) ->
  MyCurrentTerm = raft_meta:get_currentTerm(RaftMeta),
  case MyCurrentTerm > LeaderTerm of
    true ->
      ok = raft_utils:send_appendEntriesReply(From, _Success = false, RaftMeta),
      gen_raft:continue(State);
    false ->
      raft_follower:become(cleanup(State), From, RPC)
  end.

%%%*_/ internal functions ======================================================

start_new_term_election(#?state{ raft_meta  = RaftMeta
                               , raft_logs  = RaftLogs
                               , raft_state = Candidate0
                               } = State) ->
  #?candidate{election_timer = ?undef} = Candidate0, %% assert
  {ok, NewRaftMeta} = raft_meta:bump_term(RaftMeta),
  %% send to all peers including myself
  Peers = raft_meta:get_all_members(NewRaftMeta),
  LastTick = raft_logs:get_lastTick(RaftLogs),
  Request = raft_meta:make_requestVoteRPC(NewRaftMeta, LastTick),
  ok = raft_utils:multi_cast(Peers, Request),
  ElectionTimeout = raft_utils:get_election_timeout(State),
  TimerRef = raft_utils:maybe_start_election_timer(RaftMeta, ElectionTimeout),
  Candidate = Candidate0#?candidate{election_timer = TimerRef},
  gen_raft:continue(State#?state{ raft_meta  = NewRaftMeta
                                , raft_state = Candidate
                                }).

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
    false -> gen_raft:continue(NewState)
  end;
handle_requestVoteReply(_FromPeer, _VoteGranted = false, State) ->
  gen_raft:continue(State).

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
  cancel_election_timer(State).

become_leader(#?state{} = State) ->
  NewState = cleanup(State),
  LeaderInitArgs = [],
  raft_leader:become(LeaderInitArgs, NewState).

loginfo(State, Fmt, Args) -> raft_utils:log(info, State, Fmt, Args).


