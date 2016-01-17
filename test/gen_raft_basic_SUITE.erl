-module(gen_raft_basic_SUITE).
-behaviour(gen_raft).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(max_no_leader_emerge_tolerance_seconds, 10).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  _ = random:seed(os:timestamp()),
  error_logger:info_msg("\n============= ~p ============\n", [Case]),
  Config.

end_per_testcase(_Case, Config) -> Config.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

%%%_* gen_rat callbacks ========================================================

-record(state,
        { name
        , tester_pid
        }).

init([Name, Pid]) ->
  LastTick = undefined, %% empty state machine
  State = #state{ name       = Name
                , tester_pid = Pid
                },
  {ok, LastTick, State}.

terminate(_Reason, _State) -> ok.

elected(#state{tester_pid = Pid} = State) ->
  Pid ! {elected, self()},
  {ok, State}.

stepdown(#state{tester_pid = Pid} = State) ->
  Pid ! {stepdown, self()},
  {ok, State}.

%%%_* Test functions ===========================================================

t_1_node_cluster(Config) when is_list(Config) ->
  {ok, Dir} = file:get_cwd(),
  Name = peer1,
  MyId = {Name, node()},
  ok = gen_raft:create_node(Dir, MyId, []),
  RaftInitArgs = [ {metadata_dir, Dir} ],
  {ok, Pid} = start_gen_raft(Name, RaftInitArgs),
  receive
    {elected, Pid} ->
      ok
  after 2000 ->
    ct:fail(timeout)
  end,
  ?assert(gen_raft:is_leader(Pid)),
  ok = gen_raft:stop(Pid),
  ok.

t_2_nodes_cluster(Config) when is_list(Config) ->
  {ok, Dir} = file:get_cwd(),
  Peer1 = {peer1, node()},
  Peer2 = {peer2, node()},
  ok = gen_raft:create_node(Dir, Peer1, [Peer2]),
  ok = gen_raft:create_node(Dir, Peer2, [Peer1]),
  RaftInitArgs = [ {metadata_dir, Dir} ],
  {ok, Pid1} = start_gen_raft(peer1, RaftInitArgs),
  {ok, Pid2} = start_gen_raft(peer2, RaftInitArgs),
  Leader =
    receive
      {elected, Pid} ->
        Pid
    after 2000 ->
      ct:fail(timeout)
    end,
  timer:sleep(2000),
  {Leader, Follower} = case Leader of
                          Pid1 -> {Pid1, Pid2};
                          Pid2 -> {Pid2, Pid1}
                       end,
  ?assert(gen_raft:is_leader(Leader)),
  ?assertNot(gen_raft:is_leader(Follower)),
  ok = gen_raft:stop(Pid1),
  timer:sleep(100),
  ok = gen_raft:stop(Pid2),
  ok.

t_3_nodes_cluster(Config) when is_list(Config) ->
  x_nodes_cluster(3).

t_4_to_11_nodes_cluster(Config) when is_list(Config) ->
  X = 3 + random:uniform(8),
  x_nodes_cluster(X).

t_12_to_23_nodes_cluster(Config) when is_list(Config) ->
  X = 11 + random:uniform(12),
  x_nodes_cluster(X).

t_24_to_67_nodes_cluster(Config) when is_list(Config) ->
  X = 23 + random:uniform(44),
  x_nodes_cluster(X).

x_nodes_cluster(X) ->
  {ok, Dir} = file:get_cwd(),
  NameF = fun(I) -> "p" ++ lists:flatten(io_lib:format("~.3.0w", [I])) end,
  Names = [list_to_atom(NameF(I)) || I <- lists:seq(1, X)],
  Ids = [{Name, node()} || Name <- Names],
  lists:foreach(
    fun(Id) ->
      ok = gen_raft:create_node(Dir, Id, Ids)
    end, Ids),
  RaftInitArgs0 =
    if
      X > 23  -> [{election_timeout, 4000}];
      X > 11  -> [{election_timeout, 1000}];
      true    -> [{election_timeout, 500}]
    end,
  RaftInitArgs = [{metadata_dir, Dir} | RaftInitArgs0],
  Pids =
    lists:map(
      fun(Name) ->
        {ok, Pid} = start_gen_raft(Name, RaftInitArgs),
        Pid
      end, Names),
  shutdown_cluster(Pids, _Quorum = (X div 2) + 1).

%%%_* Help functions ===========================================================

start_gen_raft(Name, RaftInitArgs) ->
  gen_raft:start_link(Name, RaftInitArgs,
                      _CbMod = ?MODULE,
                      _CbArgs = [Name, self()],
                      _Options = []).

shutdown_cluster(Pids, Quorum) when length(Pids) < Quorum ->
  ok = assert_no_leader(Pids, 5000),
  lists:foreach(fun(Pid) -> gen_raft:stop(Pid) end, Pids);
shutdown_cluster(Pids, Quorum) ->
  LeaderPid = wait_for_leader(Pids),
  ok = gen_raft:stop(LeaderPid),
  shutdown_cluster(lists:delete(LeaderPid, Pids), Quorum).

assert_no_leader(Pids, Timeout) ->
  try
    Pid = wait_for_leader(Pids, Timeout),
    ct:fail("leader emerged when not supposed to, pid=~p", [Pid])
  catch throw : timeout ->
    ok
  end.

wait_for_leader(Pids) ->
  MaxTimeToWait = timer:seconds(?max_no_leader_emerge_tolerance_seconds),
  wait_for_leader(Pids, MaxTimeToWait).

wait_for_leader(Pids, MaxTimeToWait) ->
  receive
    {elected, Pid} ->
      try
        assert_cluster_member_roles(Pid, Pids),
        Pid
      catch throw : bad_state ->
        wait_for_leader(Pids, MaxTimeToWait)
      end;
    {stepdown, _Pid} ->
      wait_for_leader(Pids, MaxTimeToWait)
  after MaxTimeToWait ->
    throw(timeout)
  end.

assert_cluster_member_roles(Leader, Pids) ->
  ?assert(lists:member(Leader, Pids)),
  {StateName, LeaderTerm} = gen_raft:get_state_and_term(Leader),
  case StateName =:= raft_leader of
    true ->
      ok;
    false ->
      error_logger:warning_msg("~p is expected to be in leader state "
                               "but perhaps just stepped down",
                               [get_name(Leader)]),
      throw(bad_state)
  end,
  Followers = lists:delete(Leader, Pids),
  lists:foreach(fun(Pid) -> assert_follower(Pid, LeaderTerm) end,
                Followers).

assert_follower(Pid, LeaderTerm) ->
  assert_follower(Pid, LeaderTerm, _Retry = 0).

-define(RETRY_DELAY_MS, 100).
-define(MAX_RETRY, 10).

assert_follower(Pid, LeaderTerm, Retry) when Retry >= ?MAX_RETRY ->
  Name = get_name(Pid),
  {StateName, Term} = gen_raft:get_state_and_term(Pid),
  error_logger:warning_msg("~p is expected to be in state raft_follower "
                           "with term synced, but it's in state ~p, "
                           "term=~p leader_term=~p",
                           [Name, StateName, Term, LeaderTerm]),
  throw(bad_state);
assert_follower(Pid, LeaderTerm, Retry) ->
  {StateName, Term} = gen_raft:get_state_and_term(Pid),
  case StateName =:= raft_follower andalso Term =:= LeaderTerm of
    true ->
      ok;
    false ->
      timer:sleep(?RETRY_DELAY_MS),
      assert_follower(Pid, LeaderTerm, Retry+1)
  end.

get_name(Pid) ->
  [{registered_name, Name}] = process_info(Pid, [registered_name]),
  Name.

