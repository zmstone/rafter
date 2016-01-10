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
           #?state{ raft_meta  = RaftMeta
                  , raft_state = RaftState
                  } = State) ->
  #?follower{ election_timeout = ElectionTimeout
            , election_timer   = TimerRef
            } = RaftState,
  {MsgRef, _Tref} = TimerRef, %% assert
  true = raft_meta:is_cluster_member(RaftMeta), %% assert
  CandidateInitArgs = [ {election_timeout, ElectionTimeout}
                      ],
  NewState = State#?state{raft_state = ?undef},
  raft_candidate:become(CandidateInitArgs, NewState);
handle_msg(From, #requestVoteRPC{} = RPC, State) ->
  {ok, NewState} = raft_utils:handle_requestVoteRPC(From, RPC, State),
  gen_raft:continue(NewState).

%%%*_/ internal functions ======================================================

getarg(Name, Args, Default) ->
  proplists:get_value(Name, Args, Default).

%% @private Establish connection to all peer erlang nodes.
-spec connect_peer_nodes(raft_meta()) -> ok.
connect_peer_nodes(Meta) ->
  lists:foreach(
    fun(?raft_peer(Node, _Name)) ->
      _ = net_kernel:connect_node(Node)
    end, raft_meta:get_peer_members(Meta)).

