-module(raft_utils).

-export([ cancel_election_timer/1
        , cast/2
        , get_election_timeout/1
        , handle_requestVoteRPC/3
        , log/4
        , maybe_start_election_timer/2
        , multi_cast/2
        , send_appendEntriesReply/3
        , send_requestVoteReply/3
        ]).

-include("gen_raft_private.hrl").

%%%*_/ APIs ====================================================================

%% @doc Start election timer only if I am a cluster member.
%% Two possible reasons for me to initialize when I'm not a member:
%% 1. I am a newly initialized node and waiting to be added to the cluster
%% 2. Or it is a node that has recently been removed from the cluster
%%    but failed to shutdown properly and got restarted (by supervisor?)
%% @end
-spec maybe_start_election_timer(raft_meta(), timer:time()) ->
        ?undef | timer_ref().
maybe_start_election_timer(RaftMeta, ElectionTimeout) ->
  case raft_meta:is_cluster_member(RaftMeta) of
    true  -> start_election_timer(ElectionTimeout);
    false -> ?undef
  end.

%% @doc Cancel election timer if it is still running.
%% In case it is already timed out when this functions is
%% called, the already queued timeout message is flushed
%% from self() message queue.
%% @end
-spec cancel_election_timer(?undef | timer_ref()) -> ok.
cancel_election_timer(?undef) -> ok;
cancel_election_timer({MsgRef, Tref}) ->
  %% cancel timer
  _ = timer:cancel(Tref),
  %% flush message
  receive
    ?raft_msg(self, #electionTimeout{ref = MsgRef}) ->
      ok
  after 0 ->
    ok
  end.

%% @doc Send a message to (maybe remote) peer. Ignore exception if any.
-spec cast(pid() | raft_peer(), term()) -> ok.
cast(Dst, Msg) ->
  try erlang:send(Dst, Msg)
  catch _ : _ -> ok
  end,
  ok.

%% @doc Send a message to (maybe remote) peers. Ignore exceptions if any.
-spec multi_cast(raft_peers(), raft_msg()) -> ok.
multi_cast(Peers, Msg) ->
  lists:foreach(fun(Peer) -> ok = cast(Peer, Msg) end,
                ordsets:to_list(Peers)).

%% @doc Handle requestVoteRPC, update raft meta, send reply.
-spec handle_requestVoteRPC(raft_peer(), #requestVoteRPC{}, #?state{}) ->
        {ok, VoteGranted :: boolean(), #?state{}}.
handle_requestVoteRPC(FromPeer,
                      #requestVoteRPC{ newTerm  = ProposedTerm
                                     , lastTick = LastTick
                                     },
                      #?state{ raft_meta = RaftMeta
                             , raft_logs = RaftLogs
                             } = State) ->
  MyLastTick = raft_logs:get_lastTick(RaftLogs),
  {VoteGranted, NewRaftMeta} =
    raft_meta:maybe_grant_vote(FromPeer, ProposedTerm, LastTick,
                               MyLastTick, RaftMeta),
  {ok, NewState} = gen_raft:put_raft_meta(NewRaftMeta, State),
  ok = send_requestVoteReply(FromPeer, VoteGranted, NewRaftMeta),
  {ok, VoteGranted, NewState}.

%% @doc Reply requestVoteRPC.
-spec send_requestVoteReply(raft_peer(), boolean(), raft_meta()) -> ok.
send_requestVoteReply(ReplyToPeer, VoteGranted, RaftMeta) ->
  MyId = raft_meta:get_myId(RaftMeta),
  MyTerm = raft_meta:get_currentTerm(RaftMeta),
  Reply = ?raft_msg(MyId, #requestVoteReply{ voteGranted = VoteGranted
                                           , peerTerm    = MyTerm
                                           }),
  ok = cast(ReplyToPeer, Reply).


%% @doc Wrapper around error_logger.
log(Level, State, Fmt, Args) ->
  LogHeader = log_header(State),
  NewFmt = "~s " ++ Fmt,
  NewArgs = [LogHeader | Args],
  case Level of
    info  -> error_logger:info_msg(NewFmt, NewArgs);
    warn  -> error_logger:warning_msg(NewFmt, NewArgs);
    error -> error_logger:error_msg(NewFmt, NewArgs)
  end.

-spec get_election_timeout(#?state{} | raft_init_args()) -> timer:time().
get_election_timeout(#?state{init_args = InitArgs}) ->
  get_election_timeout(InitArgs);
get_election_timeout(InitArgs) when is_list(InitArgs) ->
  proplists:get_value(election_timeout, InitArgs, ?DEFAULT_ELECTION_TIMEOUT).


-spec send_appendEntriesReply(raft_peer(), boolean(), raft_meta()) -> ok.
send_appendEntriesReply(From, Success, RaftMeta) ->
  MyId = raft_meta:get_myId(RaftMeta),
  MyCurrentTerm = raft_meta:get_currentTerm(RaftMeta),
  Reply = #appendEntriesReply{ peerTerm = MyCurrentTerm
                             , success  = Success
                             },
  raft_utils:cast(From, ?raft_msg(MyId, Reply)).

%%%*_/ internal functions ======================================================

-spec log_header(#?state{}) -> iodata().
log_header(#?state{name = Name, raft_state = ?undef}) ->
  io_lib:format("[~p]", [Name]);
log_header(#?state{ name       = Name
                  , raft_meta  = RaftMeta
                  , raft_state = RaftState
                  }) ->
  StateName = case element(1, RaftState) of
                raft_follower  -> follower;
                raft_candidate -> candidate;
                raft_leader    -> leader
              end,
  Term = raft_meta:get_currentTerm(RaftMeta),
  io_lib:format("[~p ~p term=~p state=~p]", [Name, self(), Term, StateName]).

%% @private start election timer.
%% Return a tuple of reference and timer reference
%% @end
-spec start_election_timer(timer:time()) -> timer_ref().
start_election_timer(BaseTime) ->
  Timeout = randomised_election_timeout(BaseTime),
  MsgRef = make_ref(),
  Msg = ?raft_msg(self, #electionTimeout{ref = MsgRef}),
  {ok, Tref} = timer:send_after(Timeout, Msg),
  {MsgRef, Tref}.

%% @private Get randomised election timeout value.
-spec randomised_election_timeout(timer:time()) -> timer:time().
randomised_election_timeout(BaseTime) ->
  random:uniform(BaseTime).

%%%*_/ tests ===================================================================

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

election_timer_test() ->
  Ref = start_election_timer(100),
  ok = cancel_election_timer(Ref),
  receive
    Msg -> throw(Msg)
  after 200 ->
    ok
  end.

-endif.
