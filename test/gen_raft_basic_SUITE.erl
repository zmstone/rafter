-module(gen_raft_basic_SUITE).
-behaviour(gen_raft).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
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

t_2_node_cluster(Config) when is_list(Config) ->
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
      ct:faile(timeout)
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

t_3_node_cluster(Config) when is_list(Config) ->
  x_node_cluster(3).

t_x_node_cluster(Config) when is_list(Config) ->
  _ = random:seed(os:timestamp()),
  X = random:uniform(32),
  x_node_cluster(X).

x_node_cluster(X) ->
  {ok, Dir} = file:get_cwd(),
  NameF = fun(I) -> "p" ++ lists:flatten(io_lib:format("~.2.0w", [I])) end,
  Names = [list_to_atom(NameF(I)) || I <- lists:seq(1, X)],
  Ids = [{Name, node()} || Name <- Names],
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
  timer:sleep(5000),
  ?assert(lists:member(Leader, Pids)),
  ?assert(gen_raft:is_leader(Leader)),
  ?assert(lists:all(fun(Pid) -> not gen_raft:is_leader(Pid) end,
                    lists:delete(Leader, Pids))),
  lists:foreach(
    fun(Pid) ->
      gen_raft:stop(Pid),
      timer:sleep(100)
    end, Pids),
  ok.

%%%_* Help functions ===========================================================

start_gen_raft(Name, RaftInitArgs) ->
  gen_raft:start_link(Name, RaftInitArgs,
                      _CbMod = ?MODULE,
                      _CbArgs = [Name, self()],
                      _Options = []).

