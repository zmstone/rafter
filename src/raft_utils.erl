-module(raft_utils).

-export([ cancel_election_timer/1
        , cast/2
        , maybe_start_election_timer/2
        , multi_cast/2
        , safe_send/2
        ]).

-include("gen_raft_private.hrl").

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
    ?raft_electionTimeout(MsgRef) ->
      ok
  after 0 ->
    ok
  end.

%% @doc Send a message to (maybe remote) peer. Ignore exception if any.
-spec cast(raft_peer(), term()) -> ok.
cast(?raft_peer(Node, Name), Msg) ->
  Dst = case Node =:= node() of
          true  -> Name;
          false -> {Name, Node}
        end,
  try erlang:send(Dst, Msg)
  catch _ : _ -> ok
  end,
  ok.

%% @doc Send a message to (maybe remote) peers. Ignore exceptions if any.
-spec multi_cast(raft_peers(), raft_msg()) -> ok.
multi_cast(Peers, Msg) ->
  lists:foreach(fun(Peer) -> ok = cast(Peer, Msg) end,
                ordsets:to_list(Peers)).


%% @doc Send a message to pid without crashing.
-spec safe_send(pid(), term()) -> ok.
safe_send(Pid, Msg) ->
  try Pid ! Msg
  catch _ : _ -> ok
  end,
  ok.

%%%*_/ internal functions ======================================================

%% @private start election timer.
%% Return a tuple of reference and timer reference
%% @end
-spec start_election_timer(timer:time()) -> timer_ref().
start_election_timer(BaseTime) ->
  Timeout = randomised_election_timeout(BaseTime),
  MsgRef = make_ref(),
  Tref = timer:send_after(Timeout, ?raft_electionTimeout(MsgRef)),
  {MsgRef, Tref}.


%% @private Get randomised election timeout value.
%% time varies from BaseTime to 2*BaseTime.
-spec randomised_election_timeout(timer:time()) -> timer:time().
randomised_election_timeout(BaseTime) ->
  random:uniform(BaseTime) + BaseTime.

