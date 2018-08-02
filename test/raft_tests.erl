-module(raft_tests).

-include_lib("eunit/include/eunit.hrl").
-include("raft_cfg.hrl").

basic_test() ->
  Cfg = cfg(basic_test),
  {ok, Pid} = raft:start_link(Cfg),
  ok = raft:shutdown(Pid).

cfg(MyId) ->
  Dir = filename:join(["tdata", MyId]),
  #{ ?data_dir => Dir
   , ?peer_conn_module => raft_peer_rn
   , ?my_id => MyId
   , ?initial_members => [MyId]
   }.

