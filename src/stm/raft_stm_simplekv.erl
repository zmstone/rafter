-module(raft_stm_simplekv).
-behaviour(raft_stm).

-export([load/1, get_last_lid/1, make_wal/1, apply_wal/3]).

-include("raft_int.hrl").

load(#{dir := Dir}) ->
  File = filename:join(Dir, "data.eterm"),
  ok = filelib:ensure_dir(File),
  case file:consult(File) of
    {ok, [#{last_lid := LastLid, data := Data}]} ->
      #{last_lid => LastLid,
        data => Data
       };
    {error, enoent} ->
      #{last_lid => undefined,
        data => #{}
       }
  end.

get_last_lid(#{last_lid := LastLid}) -> LastLid.

make_wal({put, Key, Value}) ->
  term_to_binary({put, Key, Value}).

apply_wal(#{last_lid := LastLid,
            data := Data
           } = Stm, Lid, Wal) ->
  ?LID(LastGnr, LastIdx) = LastLid,
  ?LID(Gnr, Idx) = Lid,
  ok = ?ASSERT_MONOTONIC_GNR(LastGnr, Gnr),
  ok = ?ASSERT_CONSECUTIVE_IDX(LastIdx, Idx),
  {put, Key, Value} = binary_to_term(Wal),
  Stm#{last_lid := Lid,
       data := Data#{Key => Value}
      }.

