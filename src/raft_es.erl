%% escript module
-module(raft_es).

-export([main/1, start/0, start/1, start_one/2, kill_one/1]).

-include("raft_cfg.hrl").

main([]) -> run(3);
main([N]) -> run(N).

run(N) ->
  start(N),
  loop_until_exit().

loop_until_exit() ->
  {ok, [Line]} = io:fread("", "~s"),
  case Line of
    "exit" -> ok;
    _ -> loop_until_exit()
  end.

start() -> start(3).

start([N]) when is_atom(N) ->
  start(atom_to_list(N));
start(N) when is_list(N) ->
  start(list_to_integer(N));
start(N) when is_integer(N) ->
  logger:set_primary_config(level, info),
  Ids = lists:map(fun make_id/1, lists:seq(1, N)),
  lists:map(fun(I) -> start_one(I, Ids) end, Ids).

start_one(I, N) when is_integer(I) ->
  Ids = lists:map(fun make_id/1, lists:seq(1, N)),
  start_one(make_id(I), Ids);
start_one(Id, Ids) ->
  Cfg = cfg(Id, Ids),
  {ok, Pid} = raft:start(Cfg),
  Pid.

make_id(I) ->
  list_to_atom(atom_to_list(?MODULE) ++ "_" ++ integer_to_list(I)).

cfg(MyId, AllIds) ->
  Dir = filename:join(["tdata", MyId]),
  #{ ?data_dir => Dir
   , ?peer_conn_module => raft_peer_rn
   , ?my_id => MyId
   , ?initial_members => AllIds
   }.

kill_one(I) ->
  Name = make_id(I),
  Pid = whereis(Name),
  is_pid(Pid) andalso erlang:exit(Pid, kill).

