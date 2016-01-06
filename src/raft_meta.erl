-module(raft_meta).

-export([ create/2
        , deserialize/2
        , serialize/1
        ]).

-export([ get_all_members/1
        , get_currentTerm/1
        , get_lastApplied/1
        , get_myId/1
        , get_peer_members/1
        , get_votedFor/1
        , make_requestVoteRPC/1
        , is_cluster_member/1
        ]).

-export([ bump_term/1
        , maybe_update_currentTerm/2
        , set_votedFor/2
        ]).

-export_type([ meta/0
             ]).

-include("gen_raft_private.hrl").

-type changingMember() :: ?undef | {add, raft_peer()} | {del, raft_peer()}.

%% Mandatory Field initialization error.
-define(MF(FN), erlang:error({mandator_field_init_error, FN})).

-record(meta,
        { myId           = ?MF(myId)        :: raft_peer()
        , members        = ?MF(members)     :: raft_peers()
        , changingMember = ?undef           :: changingMember()
        , votedFor       = ?undef           :: ?undef | raft_peer()
        , currentTerm    = 0                :: raft_term()
        , lastApplied    = ?raft_tick(0, 0) :: raft_tick()
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
  NewMeta = Meta#meta{ currentTerm = CurrentTerm + 1
                     , votedFor    = ?undef
                     },
  {ok, NewMeta}.

-spec maybe_update_currentTerm(meta(), raft_term()) -> meta().
maybe_update_currentTerm(#meta{currentTerm = CurrentTerm} = Meta, NewTerm) ->
  Meta#meta{currentTerm = max(CurrentTerm, NewTerm)}.

-spec set_votedFor(meta(), raft_peer()) -> meta().
set_votedFor(Meta, VotedFor) ->
  Meta#meta{votedFor = VotedFor}.

-spec get_myId(meta()) -> raft_peer().
get_myId(#meta{myId = MyId}) -> MyId.

-spec get_currentTerm(meta()) -> raft_term().
get_currentTerm(#meta{currentTerm = Term}) -> Term.

-spec get_lastApplied(meta()) -> raft_tick().
get_lastApplied(#meta{lastApplied = LastApplied}) -> LastApplied.

-spec get_votedFor(meta()) -> ?undef | raft_peer().
get_votedFor(#meta{votedFor = VotedFor}) -> VotedFor.

-spec make_requestVoteRPC(meta()) -> raft_requestVoteRPC().
make_requestVoteRPC(#meta{} = Meta) ->
  ?raft_requestVoteRPC(_FromPeer    = get_myId(Meta),
                       _NewTerm     = get_currentTerm(Meta),
                       _LastApplied = get_lastApplied(Meta)).

%%%*_/ internal functions ======================================================


