%% @doc This module is an abstraction API over connections to peer nodes.

-module(raft_peers).

%% management
-export([ init/3
        , peer_connected/3
        , peer_down/2
        , spawn_connectors/4
        , stop_connector/2
        , stop_connector/1
        ]).

%% messaging
-export([ broadcast/2
        , cast/3
        ]).

%% misc
-export([ is_majority_present/1
        , unify_id/2
        ]).

-export_type([id/0, opts/0, peers/0]).

-include("raft_int.hrl").
-include("raft_msgs.hrl").

-type opts() :: raft_peer_rn:opts().
-type id() :: raft_peer_rn:id().
-type message() :: term().
-type send_fun() :: fun((message()) -> ok).
-opaque peers() :: #{ module := module()
                    , opts := opts()
                    , connectors := #{id() => pid()}
                    , connected := #{id() => send_fun()}
                    }.

%%%*_/ APIs ====================================================================

%% @doc Send message to all connected peers.
-spec broadcast(peers(), message()) -> ok.
broadcast(#{connected := Connected}, Msg) ->
  lists:foreach(fun(SendF) -> SendF(Msg) end, maps:values(Connected)).

%% @doc Send message to a peer.
cast(#{connected := Connected}, Id, Msg) ->
  SendFun = maps:get(Id, Connected),
  SendFun(Msg).

%% @doc Unify cluster member ID.
-spec unify_id(module(), term()) -> id().
unify_id(Module, ID) -> Module:unify_id(ID).

%% @doc Return a unified id(), and maybe do some side-effects.
-spec init(module(), id(), opts()) -> peers().
init(Module, MyId, Opts) ->
  ok = Module:init_self(MyId, Opts),
  #{ module => Module
   , opts => Opts
   , connectors => #{}
   , connected => #{}
   }.

%% @doc Add a connected peer.
-spec peer_connected(peers(), id(), send_fun()) -> peers().
peer_connected(#{connected := Connected} = Peers, Id, SendFun) ->
  Peers#{connected := maps:put(Id, SendFun, Connected)}.

%% @doc Delete a disconnected peer.
-spec peer_down(peers(), id()) -> peers().
peer_down(#{connected := Connected} = Peers, Id) ->
  Peers#{connected := maps:remove(Id, Connected)}.

%% @doc Return true if connected to no less than a half of the cluster members.
-spec is_majority_present(peers()) -> boolean().
is_majority_present(#{ connectors := Connectors
                     , connected := Connected
                     }) ->
  ClusterSize = 1 + maps:size(Connectors), %% including self
  ConnectedCount = 1 + maps:size(Connected), %% including self
  ?IS_MAJORITY(ConnectedCount, ClusterSize).

%% @doc Paernt should expect `?peer_connected(Id, SendFun)' message when connection
%% is established, and `?peer_down(Id)' message when connection is down.
-spec spawn_connectors(id(), peers(), [id()], pid()) -> peers().
spawn_connectors(MyId,
                 #{ module := Module
                  , opts := Opts
                  } = Peers, Ids, Parent) ->
  Connectors = [{Id, Module:spawn_connector(MyId, Id, Opts, Parent)} || Id <- Ids],
  Peers#{connectors := maps:from_list(Connectors)}.

-spec stop_connector(peers(), id()) -> peers().
stop_connector(#{connectors := Connectors} = Peers, Id) ->
  case maps:get(Id, Connectors, false) of
    Pid when is_pid(Pid) ->
      ok = stop_connector(Pid),
      Peers#{connectors := maps:remove(Id, Connectors)};
    false ->
      Peers
  end.

%%%*_/ internal functions ======================================================

stop_connector(Pid) ->
  Mref = erlang:monitor(process, Pid),
  Pid ! ?stop_connector,
  receive
    {'DOWN', Mref, process, Pid, _} ->
      ok
  after
    1000 ->
      unlink(Pid),
      erlang:exit(Pid, kill),
      ok
  end.

