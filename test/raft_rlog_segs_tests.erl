-module(raft_rlog_segs_tests).

-include("raft_int.hrl").
-include("raft_cfg.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MOD, raft_rlog_segs).

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
        Rlog0 = open(Dir, #{?rlog_seg_bytes => 10}),
        Rlog1 = append(Rlog0, 0, 0, <<0, 0, 0>>),
        Rlog2 = append(Rlog1, 0, 1, <<0, 0, 0>>),
        {Epoch, Bin} = ?MOD:read(Rlog2, 0, 10),
        ?assertEqual(0, Epoch),
        ?assertEqual([{0, <<0, 0, 0>>}], decode_entries(Bin)),
        close(Rlog2)
      end)
   },
   {"open existing",
    ?WITH_TMP_DIR(Dir,
      begin
        Entry = binary:copy(<<0>>, 10),
        Cfg = #{?rlog_seg_bytes => 50},
        Rlog0 = open(Dir, Cfg),
        Rlog1 = append(Rlog0, 0, 0, Entry),
        ok = close(Rlog1),
        Rlog2 = open(Dir, Cfg),
        Rlog3 = append(Rlog2, 0, 1, Entry),
        {0, Bin1} = read(Rlog3, 0),
        ?assertEqual([{0, Entry}], decode_entries(Bin1)),
        {0, Bin2} = read(Rlog3, 1),
        ?assertEqual([{1, Entry}], decode_entries(Bin2)),
        Rlog4 = append(Rlog3, 3, 2, <<"foo">>),
        Rlog5 = append(Rlog4, 3, 3, <<"bar">>),
        {0, Bin1} = read(Rlog5, 0),
        {0, Bin2} = read(Rlog5, 1),
        {3, Bin3} = read(Rlog5, 2),
        {3, Bin4} = read(Rlog5, 2, 999999999),
        ?assertEqual([{2, <<"foo">>}], decode_entries(Bin3)),
        ?assertEqual([{2, <<"foo">>}, {3, <<"bar">>}], decode_entries(Bin4)),
        ok = close(Rlog4)
      end)
   }
  ].

truncate_incomplete_header_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Epoch = 4,
    Rlog0 = open(Dir, #{}),
    Rlog1 = append(Rlog0, Epoch, 0, <<"foo">>),
    Fd = maps:get(fd, Rlog1),
    file:write(Fd, <<1>>),
    ok = close(Rlog1),
    Rlog2 = open(Dir, #{}),
    {Epoch, Bin1} = read(Rlog2, 0, 2),
    ?assertEqual([{0, <<"foo">>}], decode_entries(Bin1))
  end)().

truncate_incomplete_body_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Epoch = 4,
    Rlog0 = open(Dir, #{}),
    Rlog1 = append(Rlog0, Epoch, 0, <<"foo">>),
    Rlog2 = append(Rlog1, Epoch, 1, <<"bar">>),
    Fd = maps:get(fd, Rlog2),
    {ok, _} = file:position(Fd, {cur, -1}),
    ok = file:truncate(Fd),
    ok = close(Rlog2),
    Rlog3 = open(Dir, #{}),
    {Epoch, Bin1} = read(Rlog3, 0, 2),
    ?assertEqual([{0, <<"foo">>}], decode_entries(Bin1))
  end)().

truncate_empty_body_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Epoch = 4,
    Rlog0 = open(Dir, #{}),
    Rlog1 = append(Rlog0, Epoch, 0, <<"foo">>),
    Rlog2 = append(Rlog1, Epoch, 1, <<"bar">>),
    Fd = maps:get(fd, Rlog2),
    FakeHeader = <<0:8/unsigned-integer,
                  0:32/unsigned-integer,
                  1:32/unsigned-integer>>,
    ok = file:write(Fd, FakeHeader),
    ok = close(Rlog2),
    Rlog3 = open(Dir, #{}),
    {Epoch, Bin1} = read(Rlog3, 0, 2),
    ?assertEqual([{0, <<"foo">>}], decode_entries(Bin1))
  end)().

truncate_to_empty_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Epoch = 5,
    Index = 1,
    Rlog0 = open(Dir, #{}),
    Rlog1 = append(Rlog0, Epoch, Index, <<"foo">>),
    Rlog2 = append(Rlog1, Epoch + 1, Index + 1, <<"bar">>),
    Fd = maps:get(fd, Rlog2),
    {ok, _} = file:position(Fd, {cur, -2}),
    file:write(Fd, <<1>>),
    ok = close(Rlog2),
    Rlog3 = open(Dir, #{}),
    ?assertEqual(?LID(Epoch, Index), get_last_lid(Rlog3)),
    ?assertException(error, too_old, read(Rlog3, 0))
  end)().

decode_entries_bad_layout_vsn_test() ->
  ?assertException(error, {bad_layout_vsn, 1},
                   decode_entries(<<1,1,1>>)).

dir() ->
  filename:join(["tdata", integer_to_list(erlang:system_time())]).

open(Dir, Cfg) ->
  ?MOD:open(Dir, Cfg).

append(Rlog, Epoch, Index, Entry) ->
  ?MOD:append(Rlog, Epoch, [{Index, Entry}]).

decode_entries(Bin) ->
  ?MOD:decode_entries(Bin).

close(Rlog) ->
  ?MOD:close(Rlog).

read(Rlog, Index) ->
  ?MOD:read(Rlog, Index).

read(Rlog, Index, ChunkSize) ->
  ?MOD:read(Rlog, Index, ChunkSize).

get_last_lid(Rlog) ->
  ?MOD:get_last_lid(Rlog).

