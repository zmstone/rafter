-module(raft_utils).

-export([ cancel_election_timer/1
        , cast/2
        , handle_requestVoteRPC/3
        , maybe_start_election_timer/2
        , multi_cast/2
        , log/4
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
cast(?raft_peer(Node, Name), Msg) ->
  Dst = case Node =:= node() of
          true  -> Name;
          false -> {Name, Node}
        end,
  do_cast(Dst, Msg);
cast(Pid, Msg) when is_pid(Pid) ->
  do_cast(Pid, Msg).

%% @doc Send a message to (maybe remote) peers. Ignore exceptions if any.
-spec multi_cast(raft_peers(), raft_msg()) -> ok.
multi_cast(Peers, Msg) ->
  lists:foreach(fun(Peer) -> ok = cast(Peer, Msg) end,
                ordsets:to_list(Peers)).

%% @doc Handle requestVoteRPC, update raft meta, send reply.
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
  {ok, NewState}.

log(Level, #?state{name = Name, raft_state = RaftState}, Fmt, Args) ->
  StateName = case is_tuple(RaftState) of
                true  -> element(1, RaftState);
                false -> unknown_state
              end,
  NewFmt = "gen_raft ~p ~p: " ++ Fmt,
  NewArgs = [Name, StateName | Args],
  case Level of
    info  -> error_logger:info_msg(NewFmt, NewArgs);
    warn  -> error_logger:warning_msg(NewFmt, NewArgs);
    error -> error_logger:error_msg(NewFmt, NewArgs)
  end.

%%%*_/ internal functions ======================================================

%% @private start election timer.
%% Return a tuple of reference and timer reference
%% @end
-spec start_election_timer(timer:time()) -> timer_ref().
start_election_timer(BaseTime) ->
  Timeout = randomised_election_timeout(BaseTime),
  MsgRef = make_ref(),
  Msg = ?raft_msg(self, #electionTimeout{ref = MsgRef}),
  Tref = timer:send_after(Timeout, Msg),
  {MsgRef, Tref}.

%% @private Get randomised election timeout value.
%% time varies from BaseTime to 2*BaseTime.
-spec randomised_election_timeout(timer:time()) -> timer:time().
randomised_election_timeout(BaseTime) ->
  random:uniform(BaseTime) + BaseTime.

-spec do_cast(pid() | raft_name() | {raft_name(), node()}, term()) -> ok.
do_cast(Dst, Msg) ->
  try erlang:send(Dst, Msg)
  catch _ : _ -> ok
  end,
  ok.

%% @private Reply requestVoteRPC.
-spec send_requestVoteReply(raft_peer(), boolean(), raft_meta()) -> ok.
send_requestVoteReply(ReplyToPeer, VoteGranted, RaftMeta) ->
  MyId = raft_meta:get_myId(RaftMeta),
  MyTerm = raft_meta:get_currentTerm(RaftMeta),
  Reply = ?raft_msg(MyId, #requestVoteReply{ voteGranted = VoteGranted
                                           , peerTerm    = MyTerm
                                           }),
  ok = cast(ReplyToPeer, Reply).


