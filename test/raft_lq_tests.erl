-module(raft_lq_tests).

-include("raft_int.hrl").
-include_lib("eunit/include/eunit.hrl").

in_out_test() ->
  Q0 = raft_lq:new(0),
  Q1 = raft_lq:in(Q0, 1, <<"item0">>),
  ?assertEqual(?LID(1, 0), raft_lq:get_last_lid(Q1)),
  ?assertEqual([{?LID(1, 0), <<"item0">>}], raft_lq:to_list(Q1)),
  {I0, Q2} = raft_lq:out(Q1),
  ?assertEqual({?LID(1, 0), <<"item0">>}, I0),
  ?assertEqual(empty, raft_lq:out(Q2)),
  ?assertEqual(empty, raft_lq:get_last_lid(Q2)).

