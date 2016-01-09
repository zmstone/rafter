-module(raft_meta).

-export([ create/2
        , deserialize/2
        , serialize/1
        ]).

-export([ get_all_members/1
        , get_currentTerm/1
        , get_myId/1
        , get_peer_members/1
        , get_votedFor/1
        , is_cluster_member/1
        , make_requestVoteRPC/2
        ]).

-export([ bump_term/1
        , maybe_grant_vote/5
        , update_currentTerm/2
        ]).

-export_type([ meta/0
             ]).

-include("gen_raft_private.hrl").

-type changingMember() :: ?undef | {add, raft_peer()} | {del, raft_peer()}.

%% Mandatory Field initialization error.
-define(MF(FN), erlang:error({mandator_field_init_error, FN})).

-record(meta,
        { myId           = ?MF(myId)    :: raft_peer()
        , members        = ?MF(members) :: raft_peers()
        , changingMember = ?undef       :: changingMember()
        , votedFor       = ?undef       :: ?undef | {raft_term(), raft_peer()}
        , currentTerm    = 0            :: raft_term()
        }).

-opaque meta() :: #meta{}.

%%%*_/ APIs ====================================================================

%% @doc Create the very first metadata.
%% To create a new one-server cluster, call create(MyId, [MyId]).
%% To initialize a new node to be added to an existing cluster, call
%% create(MyId, PeerIdsWithoutMyself) then add myself to the cluster
%% by making addServer RPC call to the leader.
%% @end
-spec create(raft_peer(), raft_peers()) -> {ok, meta()}.
create(MyId, Members) ->
  {ok, #meta{ myId    = MyId
            , members = ordsets:from_list(Members)
            }}.

%% @doc Return 'true' if myself is a member of the current cluster.
-spec is_cluster_member(meta()) -> boolean().
is_cluster_member(#meta{myId = MyId} = Meta) ->
  ordsets:is_element(MyId, get_all_members(Meta)).

%% @doc Serialize the metadata to binary format.
-spec serialize(meta()) -> {ok, binary()}.
serialize(#meta{} = RaftMeta) ->
  Fields = record_info(fields, meta),
  Values = tl(tuple_to_list(RaftMeta)),
  KvList = lists:zip(Fields, Values),
  {ok, iolist_to_binary(io_lib:format("~p.\n", [KvList]))}.

% @doc Deserialize the metadata from binary format.
-spec deserialize(MyName :: atom(), binary()) -> {ok, meta()}.
deserialize(MyName, Binary) ->
  {ok, Tokens, _} = erl_scan:string(binary_to_list(Binary)),
  {ok, [Expr]} = erl_parse:parse_exprs(Tokens),
  {value, KvList, []} = erl_eval:expr(Expr, []),
  Fields = record_info(fields, meta),
  Values = lists:map(fun(Field) ->
                        {Field, Value} = lists:keyfind(Field, 1, KvList),
                        Value
                     end, Fields),
  Meta = list_to_tuple([meta | Values]),
  #meta{myId = ?raft_peer(Node, Name)} = Meta,
  Node =:= node() orelse erlang:error({bad_metadata, node, Node, node()}),
  Name =:= MyName orelse erlang:error({bad_metadata, name, Name, MyName}),
  {ok, Meta}.

%% @doc Get all peer members.
-spec get_peer_members(meta()) -> raft_peers().
get_peer_members(#meta{myId = Me} = Meta) ->
  AllMembers = get_all_members(Meta),
  ordsets:del_element(Me, AllMembers).


%% @doc Get all cluster members.
%% Including the changing peers, A changing peer is present when
%% adding/deleting a peer to/from the cluster.
%% Usually including myself too unless I am already removed from the cluster.
%% @end
-spec get_all_members(meta()) -> raft_peers().
get_all_members(#meta{members = Members, changingMember = ChangingMember}) ->
  case ChangingMember of
    ?undef   -> Members;
    {add, M} -> ordsets:add_element(M, Members);
    {del, M} -> ordsets:add_element(M, Members)
  end.

%% @doc Bump term.
-spec bump_term(meta()) -> {ok, meta()}.
bump_term(#meta{currentTerm = CurrentTerm} = Meta) ->
  NewMeta = Meta#meta{currentTerm = CurrentTerm + 1},
  {ok, NewMeta}.

-spec update_currentTerm(meta(), raft_term()) -> meta().
update_currentTerm(#meta{currentTerm = CurrentTerm} = Meta, NewTerm) ->
  true = (CurrentTerm < NewTerm),
  Meta#meta{currentTerm = NewTerm}.

-spec get_myId(meta()) -> raft_peer().
get_myId(#meta{myId = MyId}) -> MyId.

-spec get_currentTerm(meta()) -> raft_term().
get_currentTerm(#meta{currentTerm = Term}) -> Term.

-spec get_votedFor(meta()) -> ?undef | raft_peer().
get_votedFor(#meta{currentTerm = CurrentTerm, votedFor = VotedFor}) ->
  case VotedFor of
    {T, Peer} when T =:= CurrentTerm -> Peer;
    _                                -> ?undef
  end.

-spec make_requestVoteRPC(meta(), raft_tick()) -> raft_requestVoteRPC().
make_requestVoteRPC(#meta{} = Meta, LastTick) ->
  ?raft_requestVoteRPC(_FromPeer = get_myId(Meta),
                       _NewTerm  = get_currentTerm(Meta),
                       _LastTick = LastTick).

%% @doc Maybe or maybe not grant vote to a requestVoteRPC.
-spec maybe_grant_vote(FromPeer     :: raft_peer(),
                       ProposedTerm :: raft_term(),
                       PeerLastTick :: raft_tick(),
                       MyLastTick   :: raft_tick(),
                       RaftMeta     :: meta()) ->
        {VoteGranted :: boolean(), NewRaftMeta :: meta()}.
maybe_grant_vote(FromPeer, ProposedTerm, PeerLastTick,
                 MyLastTick, RaftMeta) ->
  VoteGranted = is_grant_vote_to(FromPeer, ProposedTerm, PeerLastTick,
                                 MyLastTick, RaftMeta),
  %% update my term if I receive a higher term in the request
  NewRaftMeta1 = maybe_update_currentTerm(RaftMeta, ProposedTerm),
  NewRaftMeta = case VoteGranted of
                  true  -> set_votedFor(NewRaftMeta1, FromPeer);
                  false -> NewRaftMeta1
                end,
  {VoteGranted, NewRaftMeta}.

%%%*_/ internal functions ======================================================

-spec set_votedFor(meta(), raft_peer()) -> meta().
set_votedFor(Meta, VotedFor) ->
  CurrentTerm = get_currentTerm(Meta),
  Meta#meta{votedFor = {CurrentTerm, VotedFor}}.

-spec maybe_update_currentTerm(meta(), raft_term()) -> meta().
maybe_update_currentTerm(#meta{currentTerm = CurrentTerm} = Meta, NewTerm) ->
  Meta#meta{currentTerm = max(CurrentTerm, NewTerm)}.

%% @private Return true if I should grant vote to a request.
-spec is_grant_vote_to(raft_peer(), raft_term(), raft_tick(),
                       raft_tick(), raft_meta()) -> boolean().
is_grant_vote_to(FromPeer, ProposedTerm,
                 PeerLastTick, MyLastTick, RaftMeta) ->
  MyCurrentTerm = get_currentTerm(RaftMeta),
  case ProposedTerm < MyCurrentTerm of
    true  ->
      false;
    false ->
      VotedFor = get_votedFor(RaftMeta),
      %% if we have not voted for someone else
      case VotedFor =:= ?undef orelse VotedFor =:= FromPeer of
        true  ->
          %% if our state machine is NOT more up-to-date
          not (MyLastTick > PeerLastTick);
        false ->
          false
      end
  end.

