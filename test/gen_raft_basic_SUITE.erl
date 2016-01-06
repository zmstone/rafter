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
        { tester_pid
        }).

init([Pid]) -> {ok, #state{tester_pid = Pid}}.

terminate(_Reason, _State) -> ok.

elected(#state{tester_pid = Pid} = State) ->
  Pid ! elected,
  {ok, State}.

%%%_* Test functions ===========================================================

t_one_node_cluster(Config) when is_list(Config) ->
  erlang:system_flag(backtrace_depth, 10),
  {ok, Dir} = file:get_cwd(),
  MyId = {node(), ?MODULE},
  ok = gen_raft:create_node(Dir, MyId, []),
  RaftInitArgs =
    [ {metadata_dir, Dir}
    ],
  {ok, Pid} =
    gen_raft:start_link(_Name = ?MODULE,
                        RaftInitArgs,
                        _CbMod = ?MODULE,
                        _CbArgs = [self()],
                        _Options = []),
  receive
    elected ->
      ok
  after 1000 ->
    ct:fail(timeout)
  end,
  ?assert(gen_raft:is_leader(Pid)),
  ok = gen_raft:stop(Pid),
  ok.

