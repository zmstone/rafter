-module(raft_lq_tests).

-include("raft_int.hrl").
-include_lib("eunit/include/eunit.hrl").

in_out_test() ->
  Q0 = raft_lq:new(0, 0),
  Q1 = raft_lq:in(<<0>>, Q0),
  {I0, Q2} = raft_lq:out(Q1),
  ?assertEqual({?LID(0, 0), <<0>>}, I0),
  ?assertEqual(empty, raft_lq:out(Q2)).

bump_epoch_test() ->
  Q0 = raft_lq:new(0, 0),
  Q1 = raft_lq:in(<<0>>, Q0),
  Q2 = raft_lq:bump_epoch(Q1),
  Q3 = raft_lq:bump_epoch(Q2),
  Q4 = raft_lq:in(<<1>>, Q3),
  ?assertEqual([{?LID(0, 0), <<0>>}, {?LID(2, 1), <<1>>}],
               raft_lq:to_list(Q4)).

