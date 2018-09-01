%% @doc This module manages connection peers as registered process names.
-module(raft_peer_rn).

-export([init_self/2, spawn_connector/4, unify_id/1]).
-export([connector_loop/5, loop_monitored/5]).
-export([send/2]).

-export_type([id/0, opts/0]).

-include("raft_int.hrl").
-include("raft_msgs.hrl").

-define(PEER_ID(Name, Node), {Name, Node}).
-define(DEFAULT_RETRY_DELAY_MS, 100).
-define(DEFAULT_FAILURE_LOG_INTERVAL, 10000).
-define(NO_TS, 0).

-opaque id() :: atom() | ?PEER_ID(RegName :: atom(), NodeName :: atom()).
-type opts() :: [ hidden | {hidden, boolean()} %% use hidden connection
                | {retry_delay_ms, integer()}
                ].

%%%*_/ APIs ====================================================================

%% @doc Unify {RegName :: atom(), Node :: atom()} representation.
-spec unify_id(term()) -> id().
unify_id(RegName) when is_atom(RegName) -> ?PEER_ID(RegName, node());
unify_id(?PEER_ID(_, _) = PeerId) -> PeerId.

%% @doc Register name for `self()', return unified peer-id format.
-spec init_self(id(), opts()) -> ok.
init_self(?PEER_ID(RegName, _Node), _Opts) ->
  true = erlang:register(RegName, self()),
  ok.

%% @doc spawn one process for a peer, the process to run in two states:
%% 1. try to establish a connection to peer node
%%    repeat until sucessful
%% 2. monitor the connection
%%    go back to state 1 when the connection is down
%% Paernt should expect `?peer_connected(Id, SendFun)' message when connection
%% is established, and `?peer_down(Id)' message when connection is down.
-spec spawn_connector(id(), id(), opts(), pid()) -> {id(), pid()}.
spawn_connector(MyId, Peer, Opts, Parent) ->
  Id = unify_id(Peer),
  Pid = erlang:spawn_link(
          fun() ->
              ?MODULE:connector_loop(MyId, Id, Parent, Opts, ?NO_TS)
          end),
  {Id, Pid}.

send(PeerId, Msg) ->
  try
    erlang:send(PeerId, Msg)
  catch
    _ : _ ->
      ok
  end,
  ok.

%%%*_/ internal functions ======================================================

maybe_log_failure(MyId, LastTs, Opts, PeerId) ->
  Interval = proplists:get_value(failure_log_interval, Opts,
                                 ?DEFAULT_FAILURE_LOG_INTERVAL),
  NowTs = erlang:system_time(millisecond),
  case NowTs - LastTs > Interval of
    true ->
      ?log_info("~p: Failed to connect ~p", [MyId, PeerId]),
      NowTs;
    false ->
      LastTs
  end.

connector_loop(MyId, ?PEER_ID(_, _) = PeerId, Parent, Opts, LastFailureLogTs0) ->
  case connect(PeerId, Opts) of
    true ->
      ?log_debug("~p: Connected to peer ~p", [MyId, PeerId]),
      Mref = erlang:monitor(process, PeerId),
      SendFun = fun(Msg) -> ?MODULE:send(PeerId, Msg) end,
      erlang:send(Parent, ?peer_connected(PeerId, SendFun)),
      ?MODULE:loop_monitored(MyId, PeerId, Parent, Mref, Opts);
    false ->
      Delay = proplists:get_value(retry_delay_ms, Opts, ?DEFAULT_RETRY_DELAY_MS),
      LastFailureLogTs = maybe_log_failure(MyId, LastFailureLogTs0, Opts, PeerId),
      receive ?stop_connector -> erlang:exit(normal) after Delay -> ok end,
      ?MODULE:connector_loop(MyId, PeerId, Parent, Opts, LastFailureLogTs)
  end.

loop_monitored(MyId, PeerId, Parent, Mref, Opts) ->
  Delay = proplists:get_value(loop_monitored_delay_ms, Opts, 1000),
  receive
    {'DOWN', Mref, process, _, Reason} ->
      ?log_info("~p: Peer ~p is down, reason:~p", [MyId, PeerId, Reason]),
      erlang:send(Parent, ?peer_down(PeerId)),
      ?MODULE:connector_loop(MyId, PeerId, Parent, Opts, ?NO_TS);
    ?stop_connector ->
      erlang:exit(normal)
  after
    Delay ->
      ?MODULE:loop_monitored(MyId, PeerId, Parent, Mref, Opts)
  end.

connect(?PEER_ID(Name, Node), Opts) ->
  connect_node(Node, Opts) andalso is_remote_pid_alive(Name, Node).

connect_node(Node, _) when Node =:= node() -> true;
connect_node(Node, Opts) ->
  case proplists:get_bool(hidden, Opts) of
    true -> net_kernel:hidden_connect_node(Node);
    false -> net_kernel:connect_node(Node)
  end.

is_remote_pid_alive(Name, Node) ->
  case rpc:call(Node, erlang, whereis, [Name], 5000) of
    Pid when is_pid(Pid) -> true;
    _ ->
      false
  end.

