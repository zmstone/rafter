-module(raft_peer_rn_tests).

-include_lib("eunit/include/eunit.hrl").
-include("raft_msgs.hrl").

-define(PEER_ID(Name, Node), {Name, Node}).

spawn_stop_connector_test() ->
  Name = fake_peer,
  Tester = self(),
  FakePeer = erlang:spawn_link(
               fun() ->
                   register(Name, self()),
                   receive Msg -> Tester ! {self(), Msg} end
               end),
  Id = ?PEER_ID(Name, node()),
  {Id, Pid} = raft_peer_rn:spawn_connector(Id, [], self()),
  SendFun = receive ?peer_connected(Id, F) -> F end,
  Msg = make_ref(),
  ok = SendFun(Msg),
  receive {FakePeer, Msg} -> ok end,
  receive ?peer_down(Id) -> ok end,
  ok = raft_peers:stop_connector(Pid),
  ?assertNot(erlang:is_process_alive(Pid)).

stop_monitored_test() ->
  Name = fake_peer,
  Tester = self(),
  FakePeer = erlang:spawn_link(
               fun() ->
                   register(Name, self()),
                   receive Msg -> Tester ! {self(), Msg} end,
                   receive stop -> ok end
               end),
  Id = ?PEER_ID(Name, node()),
  Opts = [{loop_monitored_delay_ms, 1}],
  {Id, Pid} = raft_peer_rn:spawn_connector(Id, Opts, self()),
  SendFun = receive ?peer_connected(Id, F) -> F end,
  Msg = make_ref(),
  ok = SendFun(Msg),
  receive {FakePeer, Msg} -> ok end,
  timer:sleep(100),
  ok = raft_peers:stop_connector(Pid),
  ?assertNot(erlang:is_process_alive(Pid)),
  ?assert(erlang:is_process_alive(FakePeer)),
  FakePeer ! stop,
  ok.

fail_to_connect_test() ->
  Name = tester,
  Id = ?PEER_ID(Name, 'foo@bar'),
  {Id, Pid} = raft_peer_rn:spawn_connector(Id, [{retry_delay_ms, 0}], self()),
  receive ?peer_connected(_Id, _F) -> throw(unexpected) after 2000 -> ok end,
  ok = raft_peers:stop_connector(Pid),
  ok.

fail_to_connect_hideen_test() ->
  Name = tester,
  Id = ?PEER_ID(Name, 'foo@bar'),
  {Id, Pid} = raft_peer_rn:spawn_connector(Id, [hidden], self()),
  receive ?peer_connected(_Id, _F) -> throw(unexpected) after 2 -> ok end,
  ok = raft_peers:stop_connector(Pid),
  ok.

