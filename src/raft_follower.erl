-module(raft_follower).

-export([ init/2
        , handle_msg/3
        ]).

-export_type([ follower/0
             ]).

-include("gen_raft_private.hrl").

-define(follower, ?MODULE).

-record(?follower,
        { election_timeout :: timer:time()
        , election_timer   :: ?undef | timer_ref()
        , leader_peer      :: ?undef | raft_peer()
        , leader_mpid      :: ?undef | pid()
        }).

-opaque follower() :: #?follower{}.

-spec init(binary(), raft_init_args()) -> {ok, follower()}.
init(MetadataBin, InitArgs) when is_binary(MetadataBin) ->
  [{registered_name, Name}] = process_info(self(), [registered_name]),
  {ok, RaftMeta} = raft_meta:deserialize(Name, MetadataBin),
  init(RaftMeta, InitArgs);
init(RaftMeta, InitArgs) ->
  ElectionTimeout = getarg(election_timeout, InitArgs,
                           ?DEFAULT_ELECTION_TIMEOUT),
  ok = connect_peer_nodes(RaftMeta),
  TimerRef = raft_utils:maybe_start_election_timer(RaftMeta, ElectionTimeout),
  Follower =
    #?follower{ election_timeout = ElectionTimeout
              , election_timer   = TimerRef
              },
  {ok, Follower}.

handle_msg(self, #electionTimeout{ref = MsgRef},
           #?state{ raft_meta   = RaftMeta
                  , raft_state  = Follower
                  } = State) ->
  #?follower{ election_timeout = ElectionTimeout
            , election_timer   = TimerRef
            , leader_peer      = LeaderPeer
            , leader_mpid      = LeaderMpid
            } = Follower,
  ?undef = LeaderPeer, %% assert
  ?undef = LeaderMpid, %% assert
  {MsgRef, _Tref} = TimerRef, %% assert
  true = raft_meta:is_cluster_member(RaftMeta), %% assert
  CandidateInitArgs = [ {election_timeout, ElectionTimeout}
                      ],
  raft_candidate:become(CandidateInitArgs, State);
handle_msg(From, #requestVoteRPC{} = RPC, State) ->
  {ok, NewState} = raft_utils:handle_requestVoteRPC(From, RPC, State),
  gen_raft:continue(NewState);
handle_msg(From, #appendEntriesRPC{leaderTerm = LeaderTerm} = RPC,
           #?state{raft_meta = RaftMeta} = State) ->
  MyCurrentTerm = raft_meta:get_currentTerm(RaftMeta),
  case MyCurrentTerm > LeaderTerm of
    true ->
      ok = send_appendEntriesReply(From, _Success = false, RaftMeta),
      gen_raft:continue(State);
    false ->
      handle_appendEntriesRPC(From, RPC, State)
  end.

%%%*_/ internal functions ======================================================

getarg(Name, Args, Default) ->
  proplists:get_value(Name, Args, Default).

%% @private Establish connection to all peer erlang nodes.
-spec connect_peer_nodes(raft_meta()) -> ok.
connect_peer_nodes(Meta) ->
  lists:foreach(
    fun(?raft_peer(_Name, Node)) ->
      _ = net_kernel:connect_node(Node)
    end, raft_meta:get_peer_members(Meta)).

handle_appendEntriesRPC(From, RPC, State) ->
  #appendEntriesRPC{ leaderTerm   = LeaderTerm
                   , prevTick     = PrevTick
                   , entries      = Entries
                   , leaderCommit = CommitTick
                   } = RPC,
  #?state{ raft_logs  = RaftLogs
         , raft_meta  = RaftMeta
         , raft_state = Follower
         } = State,
  case raft_logs:maybe_append(RaftLogs, Entries, PrevTick, CommitTick) of
    {ok, NewRaftLogs} ->
      NewRaftMeta = raft_meta:maybe_update_currentTerm(RaftMeta, LeaderTerm),
      #?follower{election_timer = Timer} = Follower,
      ok = raft_utils:cancel_election_timer(Timer),
      Follower1 = Follower#?follower{ election_timer = ?undef
                                    , leader_peer    = From
                                    },
      NewFollower = monitor_leader(Follower1),
      NewState = State#?state{ raft_logs  = NewRaftLogs
                             , raft_meta  = NewRaftMeta
                             , raft_state = NewFollower
                             },
      gen_raft:continue(NewState);
    false ->
      ok = send_appendEntriesReply(From, _Success = false, RaftMeta),
      gen_raft:continue(State)
  end.

-spec send_appendEntriesReply(raft_peer(), boolean(), raft_meta()) -> ok.
send_appendEntriesReply(From, Success, RaftMeta) ->
  MyId = raft_meta:get_myId(RaftMeta),
  MyCurrentTerm = raft_meta:get_currentTerm(RaftMeta),
  Reply = #appendEntriesReply{ peerTerm = MyCurrentTerm
                             , success  = Success
                             },
  raft_utils:cast(From, ?raft_msg(MyId, Reply)).

%loginfo(State, Fmt, Args) -> raft_utils:log(info, State, Fmt, Args).

-spec monitor_leader(follower()) -> follower().
monitor_leader(#?follower{leader_peer = Leader} = Follower0) ->
  Follower = demonitor_leader(Follower0),
  Follower#?follower{leader_mpid = do_monitor_leader(Leader)}.

-spec demonitor_leader(follower()) -> follower().
demonitor_leader(#?follower{leader_mpid = ?undef} = Follower) -> Follower;
demonitor_leader(#?follower{leader_mpid = Pid} = Follower) ->
  _ = unlink(Pid),
  _ = exit(Pid, kill),
  Follower#?follower{leader_mpid = ?undef}.

-spec do_monitor_leader(?undef | raft_peer()) -> ?undef | pid().
do_monitor_leader(?undef) -> ?undef;
do_monitor_leader(?raft_peer(Name, Node) = Leader) ->
  Parent = self(),
  erlang:spawn_link(
    fun() ->
      Ref = erlang:monitor(process, {Name, Node}),
      receive
        {'DOWN', Ref, process, _, Reason} ->
          %% make a message as if sent from leader
          unlink(Parent),
          Parent ! ?raft_msg(Leader, #leaderDown{reason = Reason})
      end
    end).


