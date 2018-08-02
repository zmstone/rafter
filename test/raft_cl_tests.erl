-module(raft_cl_tests).

-include("raft_int.hrl").
-include("raft_cfg.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(WITH_TMP_DIR(DIR_VAR, EXPR),
        fun() ->
            DIR_VAR = dir(),
            try
              fun() -> EXPR end()
            after
              os:cmd("rm -rf " ++ DIR_VAR)
            end
        end).

append_read_test_() ->
  [{"open empty",
    ?WITH_TMP_DIR(Dir,
      begin
        CL0 = raft_cl:open(Dir, #{?cl_seg_bytes => 10}),
        CL1 = raft_cl:append(CL0, 0, 0, <<0, 0, 0>>),
        CL2 = raft_cl:append(CL1, 0, 1, <<0, 0, 0>>),
        {Epoch, Bin} = raft_cl:read(CL2, 0, 10),
        ?assertEqual(0, Epoch),
        ?assertEqual([{0, <<0, 0, 0>>}], raft_cl:decode_entries(Bin)),
        raft_cl:close(CL2)
      end)
   },
   {"open existing",
    ?WITH_TMP_DIR(Dir,
      begin
        Entry = binary:copy(<<0>>, 10),
        Cfg = #{?cl_seg_bytes => 50},
        CL0 = raft_cl:open(Dir, Cfg),
        CL1 = raft_cl:append(CL0, 0, 0, Entry),
        ok = raft_cl:close(CL1),
        CL2 = raft_cl:open(Dir, Cfg),
        CL3 = raft_cl:append(CL2, 0, 1, Entry),
        {0, Bin1} = raft_cl:read(CL3, 0),
        ?assertEqual([{0, Entry}], raft_cl:decode_entries(Bin1)),
        {0, Bin2} = raft_cl:read(CL3, 1),
        ?assertEqual([{1, Entry}], raft_cl:decode_entries(Bin2)),
        CL4 = raft_cl:append(CL3, 3, 2, <<"foo">>),
        CL5 = raft_cl:append(CL4, 3, 3, <<"bar">>),
        {0, Bin1} = raft_cl:read(CL5, 0),
        {0, Bin2} = raft_cl:read(CL5, 1),
        {3, Bin3} = raft_cl:read(CL5, 2),
        {3, Bin4} = raft_cl:read(CL5, 2, 999999999),
        ?assertEqual([{2, <<"foo">>}], raft_cl:decode_entries(Bin3)),
        ?assertEqual([{2, <<"foo">>}, {3, <<"bar">>}], raft_cl:decode_entries(Bin4)),
        ok = raft_cl:close(CL4)
      end)
   }
  ].

truncate_incomplete_header_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Epoch = 4,
    CL0 = raft_cl:open(Dir, #{}),
    CL1 = raft_cl:append(CL0, Epoch, 0, <<"foo">>),
    Fd = maps:get(fd, CL1),
    file:write(Fd, <<1>>),
    ok = raft_cl:close(CL1),
    CL2 = raft_cl:open(Dir, #{}),
    {Epoch, Bin1} = raft_cl:read(CL2, 0, 2),
    ?assertEqual([{0, <<"foo">>}], raft_cl:decode_entries(Bin1))
  end)().

truncate_incomplete_body_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Epoch = 4,
    CL0 = raft_cl:open(Dir, #{}),
    CL1 = raft_cl:append(CL0, Epoch, 0, <<"foo">>),
    CL2 = raft_cl:append(CL1, Epoch, 1, <<"bar">>),
    Fd = maps:get(fd, CL2),
    {ok, _} = file:position(Fd, {cur, -1}),
    ok = file:truncate(Fd),
    ok = raft_cl:close(CL2),
    CL3 = raft_cl:open(Dir, #{}),
    {Epoch, Bin1} = raft_cl:read(CL3, 0, 2),
    ?assertEqual([{0, <<"foo">>}], raft_cl:decode_entries(Bin1))
  end)().

truncate_empty_body_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Epoch = 4,
    CL0 = raft_cl:open(Dir, #{}),
    CL1 = raft_cl:append(CL0, Epoch, 0, <<"foo">>),
    CL2 = raft_cl:append(CL1, Epoch, 1, <<"bar">>),
    Fd = maps:get(fd, CL2),
    FakeHeader = <<0:8/unsigned-integer,
                  0:32/unsigned-integer,
                  1:32/unsigned-integer>>,
    ok = file:write(Fd, FakeHeader),
    ok = raft_cl:close(CL2),
    CL3 = raft_cl:open(Dir, #{}),
    {Epoch, Bin1} = raft_cl:read(CL3, 0, 2),
    ?assertEqual([{0, <<"foo">>}], raft_cl:decode_entries(Bin1))
  end)().

truncate_to_empty_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Epoch = 5,
    Index = 1,
    CL0 = raft_cl:open(Dir, #{}),
    CL1 = raft_cl:append(CL0, Epoch, Index, <<"foo">>),
    CL2 = raft_cl:append(CL1, Epoch + 1, Index + 1, <<"bar">>),
    Fd = maps:get(fd, CL2),
    {ok, _} = file:position(Fd, {cur, -2}),
    file:write(Fd, <<1>>),
    ok = raft_cl:close(CL2),
    CL3 = raft_cl:open(Dir, #{}),
    ?assertEqual(?LID(Epoch, Index), raft_cl:get_last_lid(CL3)),
    ?assertException(error, too_old, raft_cl:read(CL3, 0))
  end)().

decode_entries_bad_layout_vsn_test() ->
  ?assertException(error, {bad_layout_vsn, 1},
                   raft_cl:decode_entries(<<1,1,1>>)).

dir() -> filename:join(["tdata", integer_to_list(erlang:system_time())]).
