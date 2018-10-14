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
        Gnr = 1,
        Idx = 1,
        Rlog0 = open(Dir, #{?rlog_seg_bytes => 10}),
        io:format(user, "~p\n", [Rlog0]),
        %% Not allowed to start from 0
        ?assertError({non_consecutive_index, #{got := 0, last := 0}},
                     append(Rlog0, Gnr, 0, <<0>>)),
        Rlog1 = append(Rlog0, Gnr, Idx, <<0, 0, 0>>),
        Rlog2 = append(Rlog1, Gnr, Idx + 1, <<0, 0, 0>>),
        {Gnr, Bin} = ?MOD:read(Rlog2, Idx, 10),
        ?assertEqual([{Idx, <<0, 0, 0>>}], decode_entries(Bin)),
        close(Rlog2)
      end)
   },
   {"open existing",
    ?WITH_TMP_DIR(Dir,
      begin
        Gnr = 1,
        Idx = 1,
        Entry = binary:copy(<<0>>, 10),
        Cfg = #{?rlog_seg_bytes => 50},
        Rlog0 = open(Dir, Cfg),
        Rlog1 = append(Rlog0, Gnr, Idx, Entry),
        ok = close(Rlog1),
        Rlog2 = open(Dir, Cfg),
        Rlog3 = append(Rlog2, Gnr, Idx+ 1, Entry),
        {Gnr, Bin1} = read(Rlog3, Idx),
        ?assertEqual([{Idx, Entry}], decode_entries(Bin1)),
        {Gnr, Bin2} = read(Rlog3, Idx+ 1),
        ?assertEqual([{Idx+ 1, Entry}], decode_entries(Bin2)),
        Rlog4 = append(Rlog3, Gnr + 2, Idx + 2, <<"foo">>),
        Rlog5 = append(Rlog4, Gnr + 2, Idx + 3, <<"bar">>),
        {_, Bin1} = read(Rlog5, Idx),
        {_, Bin2} = read(Rlog5, Idx + 1),
        {_, Bin3} = read(Rlog5, Idx + 2),
        {_, Bin4} = read(Rlog5, Idx + 2, 999999999),
        ?assertEqual([{Idx + 2, <<"foo">>}], decode_entries(Bin3)),
        ?assertEqual([{Idx + 2, <<"foo">>}, {Idx + 3, <<"bar">>}],
                     decode_entries(Bin4)),
        ok = close(Rlog4)
      end)
   }
  ].

truncate_incomplete_header_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Gnr = 4,
    Rlog0 = open(Dir, #{}),
    Rlog1 = append(Rlog0, Gnr, 1, <<"foo">>),
    Fd = maps:get(fd, Rlog1),
    file:write(Fd, <<1>>),
    ok = close(Rlog1),
    Rlog2 = open(Dir, #{}),
    {Gnr, Bin1} = read(Rlog2, 1, 2),
    ?assertEqual([{1, <<"foo">>}], decode_entries(Bin1))
  end)().

truncate_incomplete_body_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Gnr = 4,
    Idx = 1,
    Rlog0 = open(Dir, #{}),
    Rlog1 = append(Rlog0, Gnr, Idx, <<"foo">>),
    Rlog2 = append(Rlog1, Gnr, Idx + 1, <<"bar">>),
    Fd = maps:get(fd, Rlog2),
    {ok, _} = file:position(Fd, {cur, -1}),
    ok = file:truncate(Fd),
    ok = close(Rlog2),
    Rlog3 = open(Dir, #{}),
    {Gnr, Bin1} = read(Rlog3, Idx, 2),
    ?assertEqual([{Idx, <<"foo">>}], decode_entries(Bin1))
  end)().

truncate_empty_body_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Gnr = 4,
    Idx = 1,
    Rlog0 = open(Dir, #{}),
    Rlog1 = append(Rlog0, Gnr, Idx, <<"foo">>),
    Rlog2 = append(Rlog1, Gnr, Idx + 1, <<"bar">>),
    Fd = maps:get(fd, Rlog2),
    FakeHeader = <<0:8/unsigned-integer,
                   0:32/unsigned-integer,
                   1:32/unsigned-integer>>,
    ok = file:write(Fd, FakeHeader),
    ok = close(Rlog2),
    Rlog3 = open(Dir, #{}),
    {Gnr, Bin1} = read(Rlog3, Idx, 2),
    ?assertEqual([{Idx, <<"foo">>}], decode_entries(Bin1))
  end)().

truncate_to_empty_test() ->
  ?WITH_TMP_DIR(Dir,
  begin
    Gnr = 5,
    Idx = 1,
    Rlog0 = open(Dir, #{}),
    Rlog1 = append(Rlog0, Gnr, Idx, <<"foo">>),
    Rlog2 = append(Rlog1, Gnr + 1, Idx + 1, <<"bar">>),
    Fd = maps:get(fd, Rlog2),
    {ok, _} = file:position(Fd, {cur, -2}),
    file:write(Fd, <<1>>),
    ok = close(Rlog2),
    Rlog3 = open(Dir, #{}),
    ?assertEqual(?LID(Gnr, Idx), get_last_lid(Rlog3)),
    ?assertException(error, too_old, read(Rlog3, 0))
  end)().

decode_entries_bad_layout_vsn_test() ->
  ?assertException(error, {bad_layout_vsn, 1},
                   decode_entries(<<1,1,1>>)).

dir() ->
  filename:join(["tdata", integer_to_list(erlang:system_time())]).

open(Dir, Cfg) ->
  ?MOD:open(Dir, Cfg).

append(Rlog, Gnr, Idx, Entry) ->
  ?MOD:append(Rlog, Gnr, [{Idx, Entry}]).

decode_entries(Bin) ->
  ?MOD:decode_entries(Bin).

close(Rlog) ->
  ?MOD:close(Rlog).

read(Rlog, Idx) ->
  ?MOD:read(Rlog, Idx).

read(Rlog, Idx, ChunkSize) ->
  ?MOD:read(Rlog, Idx, ChunkSize).

get_last_lid(Rlog) ->
  ?MOD:get_last_lid(Rlog).

