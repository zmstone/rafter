-module(gen_raft_basic_SUITE).
-behaviour(gen_raft).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_Case, Config) -> Config.

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

%%%_* Test functions ===========================================================

t_one_node_cluster(Config) when is_list(Config) ->
  {ok, Dir} = file:get_cwd(),
  Name = node1,
  MyId = {node(), Name},
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

t_two_node_cluster(Config) when is_list(Config) ->
  {ok, Dir} = file:get_cwd(),
  Node1 = {node(), node1},
  Node2 = {node(), node2},
  ok = gen_raft:create_node(Dir, Node1, [Node2]),
  ok = gen_raft:create_node(Dir, Node2, [Node1]),
  RaftInitArgs = [ {metadata_dir, Dir} ],
  {ok, Pid1} = start_gen_raft(node1, RaftInitArgs),
  {ok, Pid2} = start_gen_raft(node2, RaftInitArgs),
  Leader =
    receive
      {elected, Pid} ->
        Pid
    after 2000 ->
      ct:faile(timeout)
    end,
  %timer:sleep(2000),
  case Leader of
    Pid1 -> ?assert(gen_raft:is_leader(Pid1));
    Pid2 -> ?assertNot(gen_raft:is_leader(Pid2))
  end,
  ok = gen_raft:stop(Pid1),
  ok = gen_raft:stop(Pid2),
  ok.

t_three_node_cluster(Config) when is_list(Config) ->
  {ok, Dir} = file:get_cwd(),
  Names = [node1, node2, node3],
  Ids = [{node(), Name} || Name <- Names],
  lists:foreach(
    fun(Id) ->
      ok = gen_raft:create_node(Dir, Id, Ids)
    end, Ids),
  RaftInitArgs = [ {metadata_dir, Dir} ],
  Pids =
    lists:map(
      fun(Name) ->
        {ok, Pid} = start_gen_raft(Name, RaftInitArgs),
        Pid
      end, Names),
  Leader =
    receive
      {elected, Pid} ->
        Pid
    after 2000 ->
      ct:faile(timeout)
    end,
  %timer:sleep(2000),
  ?assert(lists:member(Leader, Pids)),
  ?assert(gen_raft:is_leader(Leader)),
  lists:foreach(fun(Pid) -> gen_raft:stop(Pid) end, Pids),
  ok.

%%%_* Help functions ===========================================================

start_gen_raft(Name, RaftInitArgs) ->
  gen_raft:start_link(Name, RaftInitArgs,
                      _CbMod = ?MODULE,
                      _CbArgs = [Name, self()],
                      _Options = []).

