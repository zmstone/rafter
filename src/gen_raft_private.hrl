-ifndef(GEN_RAFT_PRIVATE_HRL).
-define(GEN_RAFT_PRIVATE_HRL, true).

-define(undef, undefined).

%% The unique id of raft peers
-define(raft_peer(Node, Name), {Node, Name}).
-type raft_name() :: atom().
-type raft_peer() :: ?raft_peer(Node :: atom(), Name :: raft_name()).
-type raft_peers() :: ordsets:ordset(raft_peer()).

-define(is_raft_peer(ID),
        (is_tuple(ID)            andalso
         size(ID) =:= 2          andalso
         is_atom(element(1, ID)) andalso
         is_atom(element(2, ID)))).

-define(raft_tick(Term, Index), {Term, Index}).

-type raft_term() :: non_neg_integer().
-type raft_index() :: non_neg_integer().
-type raft_tick() :: ?raft_tick(raft_term(), raft_index()).
-type timer_ref() :: {reference(), timer:tref()}.
-type raft_meta() :: raft_meta:raft_meta().
-type serialized_raft_meta() :: binary().
-type init_arg_name() :: metadata_dir
                       | election_timeout.
-type init_arg_value() :: term().
-type raft_init_args() :: [ {init_arg_name(), init_arg_value()}
                          | init_arg_value() ].

-type raft_state() :: raft_follower:follower()
                    | raft_candidate:candidate()
                    | raft_leader:leader().

-record(gen_raft_state,
        { name       :: atom()
        , parent     :: pid()
        , cb_mod     :: module()
        , cb_state   :: term()
        , raft_meta  :: raft_meta()
        , raft_state :: raft_state()
        , meta_fd    :: file:fd()
        , debug = [] :: list()
        }).

-define(state, gen_raft_state).

%% election timout randomised from N to 2*N
%% where N is by default ?DEFAULT_ELECTION_TIMEOUT
-define(DEFAULT_ELECTION_TIMEOUT, 500).

-record(requestVoteRPC,
        { fromPeer    :: raft_peer()
        , newTerm     :: raft_term()
        , lastApplied :: raft_tick()
        }).

-record(requestVoteReply,
        { fromPeer    :: raft_peer()
        , voteGranted :: boolean()
        , peerTerm    :: raft_term()
        }).

%% message tag macros
-define(gen_raft_init(InitArgs), {'$gen_raft', {init, InitArgs}}).
-define(gen_raft_cast(Msg), {'$gen_raft', {cast, Msg}}).
-define(gen_raft_call(Ref, From, Call), {'$gen_raft', {call, Ref, From, Call}}).
-define(gen_raft_reply(Ref, Result), {'$gen_raft', {reply, Ref, Result}}).

-define(raft_electionTimeout(MsgRef), {'$raft', {electionTimeout, MsgRef}}).
-define(raft_requestVoteRPC(FromWhichPeer, ProposedTerm, LastApplied),
        {'$raft', #requestVoteRPC{ fromPeer    = FromWhichPeer
                                 , newTerm     = ProposedTerm
                                 , lastApplied = LastApplied
                                 }}).
-define(raft_requestVoteReply(FromWhichPeer, VoteGranted, PeerTerm),
        {'$raft', #requestVoteReply{ fromPeer    = FromWhichPeer
                                   , voteGranted = VoteGranted
                                   , peerTerm    = PeerTerm
                                   }}).

-type raft_msg() :: {'$raft', term()}.
-type raft_requestVoteRPC() :: {'$raft', #requestVoteRPC{}}.
-type raft_requestVoteReply() :: {'$raft', #requestVoteReply{}}.

-define(info(FMT, ARGS), error_logger:info_msg(FMT, ARGS)).
-define(warn(FMT, ARGS), error_logger:warning_msg(FMT, ARGS)).
-define(error(FMT, ARGS), error_logger:error_msg(FMT, ARGS)).

-endif.

