%% @doc Raft replication log segment files.
-module(raft_rlog_segs).

-export([open/2, close/1]).
-export([append/3, read/2, read/3]).
-export([decode_entries/1]).
-export([get_last_lid/1, cfg_keys/0]).

-export_type([segs/0, cfg_key/0, entry/0]).

-include("raft_int.hrl").
-include("raft_cfg.hrl").

-type lid() :: raft:lid().
-type idx() :: raft:idx().
-type gnr() :: raft:grn().
-type bytes() :: non_neg_integer().
-type entry() :: binary().

-type filename() :: string().
-type dir() :: filename().

-type cfg_key() :: ?rlog_seg_bytes.

-type cfg() :: #{ ?rlog_seg_bytes => bytes()
                }.

-opaque segs() :: #{ cfg := cfg()
                   , fd := false | file:fd()
                   , fd_bytes := bytes()
                   , last_lid := lid()
                   , base_lids := [lid()]
                   }.

-define(SUFFIX, "rlog").
-define(DEFAULT_SEG_BYTES, 100 bsl 20). %% 100MB

-define(LAYOUT_VSN, 0).
-define(IDX_BYTES, 8).
-define(ENTRY_HEADER_BYTES, 9).
-define(IDX_BIN(IDX), <<IDX:64/unsigned-integer>>).
-define(V0_HEADER(CRC, SIZE),
        <<?LAYOUT_VSN:8/unsigned-integer,
          CRC:32/unsigned-integer,
          SIZE:32/unsigned-integer>>).
-define(V0_BODY(SIZE, ENTRY, IDX),
        <<ENTRY:SIZE/binary, IDX:64/unsigned-integer>>).
-define(V0_LOG(CRC, SIZE, ENTRY, IDX),
        <<?LAYOUT_VSN:8/unsigned-integer,
          CRC:32/unsigned-integer,
          SIZE:32/unsigned-integer,
          ENTRY:SIZE/binary,
          IDX:64/unsigned-integer>>).
-define(V0_LOG_TAIL(CRC, SIZE, ENTRY, IDX, REST),
        <<?LAYOUT_VSN:8/unsigned-integer,
          CRC:32/unsigned-integer,
          SIZE:32/unsigned-integer,
          ENTRY:SIZE/binary,
          IDX:64/unsigned-integer,
          REST/binary>>).
-define(V0_BODY_BYTES(Size), (Size + ?IDX_BYTES)).
-define(ASSERT(Expr, Error),
        case Expr of
          true -> ok;
          false -> erlang:error(Error)
        end).

-spec cfg_keys() -> [cfg_key()].
cfg_keys() -> [?rlog_seg_bytes].

-spec open(dir(), cfg()) -> segs().
open(Dir, Cfg0) ->
  ok = filelib:ensure_dir(filename:join(Dir, "foo")),
  Cfg = apply_defaults(Cfg0#{dir => Dir}),
  case list_sort_truncate_files(Dir) of
    [] ->
      #{ cfg       => Cfg
       , fd        => false
       , fd_bytes  => 0
       , last_lid  => ?NO_PREV_LID
       , base_lids => []
       };
    [?LID(Gnr, LastBaseIdx) | _] = Lids ->
      LastFile = filename(Dir, Gnr, LastBaseIdx),
      {LastLid, Fd} = open_and_read_last_lid(LastFile),
      {ok, Bytes} = file:position(Fd, eof),
      #{ cfg       => Cfg
       , fd        => Fd
       , fd_bytes  => Bytes
       , last_lid  => LastLid
       , base_lids => Lids
       }
  end.

-spec get_last_lid(segs()) -> lid().
get_last_lid(#{last_lid := LastLid}) -> LastLid.

-spec close(segs()) -> ok | {error, any()}.
close(#{fd := false}) -> ok;
close(#{fd := Fd}) -> file:close(Fd).

-spec append(segs(), gnr(), [{idx(), entry()}]) -> segs().
append(Segs, Gnr, Entries) ->
  #{ cfg       := #{dir := Dir}
   , last_lid  := ?LID(LastGnr, LastIdx)
   , base_lids := BaseLids
   } = Segs,
  ?ASSERT(LastGnr =< Gnr, {non_monotonic_generation_nr, #{last => LastGnr, got => Gnr}}),
  {Idx, _} = hd(Entries),
  {NewLastLid, IoData} = encode_entries(LastIdx, Entries, []),
  case is_new_segment(Segs, Gnr) of
    true ->
      ok = close(Segs),
      NextFilename = filename(Dir, Gnr, Idx),
      do_append(Segs#{ fd := open_file(NextFilename)
                     , fd_bytes := 0
                     , base_lids := [?LID(Gnr, Idx) | BaseLids]
                     , last_lid := ?LID(Gnr, NewLastLid)
                     }, IoData);
    false ->
      do_append(Segs#{last_lid := ?LID(Gnr, NewLastLid)}, IoData)
  end.

%% @doc Decode entries from binary.
-spec decode_entries(binary()) -> [{idx(), entry()}].
decode_entries(Bin) ->
  decode_entries(Bin, []).

%% @doc Read one entry, call @link decode_entries/2 to decode.
-spec read(segs(), idx()) -> {gnr(), binary()}.
read(Segs, Idx) ->
  read(Segs, Idx, _ChunkSize = 0).

%% @doc Read a chunk of entries, call @link decode_entries/2 to decode.
%% The caller should ensure the index is in valid range,
%% raise `error(empty | too_old | not_seen)' exception otherwise.
-spec read(segs(), idx(), bytes()) -> {gnr(), binary()}.
read(Segs, Idx, ChunkSize) ->
  #{ base_lids := BaseLids
   , last_lid := LastLid
   , cfg := #{dir := Dir}
   , fd := Fd
   } = Segs,
  LastLid =:= false andalso error(empty),
  ?LID(LastGnr, LastIdx) = LastLid,
  Idx > LastIdx andalso error(not_seen),
  ?LID(Gnr, BaseIdx) = find_base_lid(BaseLids, Idx),
  %% force a file sync before reading the current segment file
  case Gnr =:= LastGnr of
    true -> file:sync(Fd);
    false -> ok
  end,
  File = filename(Dir, Gnr, BaseIdx),
  Chunk = seek_and_read(File, Idx - BaseIdx, ChunkSize),
  {Gnr, Chunk}.

%%%*_/ internal functions ======================================================

do_append(#{fd := Fd, fd_bytes := Bytes} = Segs, IoData) ->
  ok = file:write(Fd, IoData),
  Segs#{fd_bytes := Bytes + bytes(IoData)}.

%% Create index file if this turns out too slow
seek_and_read(File, Offset, ChunkSize) ->
  {ok, Fd} = file:open(File, [raw, read, binary]),
  try
    ok = seek(Fd, Offset),
    read_chunk(Fd, ChunkSize)
  after
    ok = file:close(Fd)
  end.

read_chunk(Fd, ChunkSize) ->
  {ok, Header} = file:read(Fd, ?ENTRY_HEADER_BYTES),
  ?V0_HEADER(_CRC, Size) = Header,
  %% ensure read at least one entry
  BytesToRead = max(Size + ?IDX_BYTES, ChunkSize - ?ENTRY_HEADER_BYTES),
  {ok, Bin} = file:read(Fd, BytesToRead),
  <<Header/binary, Bin/binary>>.

%% Seek reader fd to the start position of the n:th entry.
%% Use file:position to skip through the first n-1 entries.
seek(_Fd, 0) -> ok;
seek(Fd, N) ->
  {ok, ?V0_HEADER(_CRC, Size)} = file:read(Fd, ?ENTRY_HEADER_BYTES),
  {ok, _} = file:position(Fd, {cur, Size + ?IDX_BYTES}),
  seek(Fd, N - 1).

find_base_lid([], _Idx) -> error(too_old);
find_base_lid([?LID(Gnr, BaseIdx) | Rest], Idx) ->
  case BaseIdx > Idx of
    true -> find_base_lid(Rest, Idx);
    false -> ?LID(Gnr, BaseIdx)
  end.

encode_entries(LastIdx, [], Acc) ->
  {LastIdx, lists:reverse(Acc)};
encode_entries(LastIdx, [{Idx, Entry} | Rest], Acc0) ->
  ?ASSERT(LastIdx + 1 =:= Idx, {non_consecutive_index, #{last => LastIdx, got => Idx}}),
  Acc = [encode_entry(Idx, Entry) | Acc0],
  encode_entries(Idx, Rest, Acc).

encode_entry(Idx, Entry) ->
  CRC = erlang:crc32(Entry),
  Size = bytes(Entry),
  ?V0_LOG(CRC, Size, Entry, Idx).

decode_entries(?V0_LOG_TAIL(CRC, Size, Entry, Idx, Rest), Acc) ->
  ?ASSERT(CRC =:= erlang:crc32(Entry), bad_crc),
  decode_entries(Rest, [{Idx, Entry} | Acc]);
decode_entries(<<V:8, _/binary>>, _Acc) ->
  erlang:error({bad_layout_vsn, V});
decode_entries(_, Acc) ->
  lists:reverse(Acc).

%% open file to append log entries
-spec open_and_read_last_lid(filename()) -> {lid(), file:fd()}.
open_and_read_last_lid(Filename) ->
  ?LID(Gnr, BaseIdx) = parse_lid_from_filename(Filename),
  Fd = open_file(Filename),
  {ok, EofPos} = file:position(Fd, eof),
  {ok, _} = file:position(Fd, EofPos - ?IDX_BYTES),
  {ok, <<Idx:64/unsigned-integer>>} = file:read(Fd, ?IDX_BYTES),
  true = (Idx >= BaseIdx), % assert
  {?LID(Gnr, Idx), Fd}.

open_file(Filename) ->
  {ok, Fd} = file:open(Filename, [raw, read, write, binary]),
  Fd.

%% parse log-id (gnr + idx) from segment file name
-spec parse_lid_from_filename(filename()) -> lid().
parse_lid_from_filename(Filename) ->
  BaseName = filename:basename(Filename, "."?SUFFIX),
  [Gnr, Idx] = string:tokens(BaseName, "."),
  ?LID(list_to_integer(Gnr), list_to_integer(Idx)).

%% make segment file name from log-id (gnr + idx)
-spec filename(dir(), gnr(), idx()) -> filename().
filename(Dir, Gnr, Idx) ->
  GnrStr = lists:flatten(io_lib:format("~10.10.0w", [Gnr])),
  IdxStr = lists:flatten(io_lib:format("~20.10.0w", [Idx])),
  filename:join([Dir, GnrStr ++ "." ++ IdxStr ++ "."?SUFFIX]).

bytes(Entry) -> erlang:iolist_size(Entry).

%% 1. list all files having ?SUFFIX suffix
%% 2. sort the list result
%% 3. truncate the last one having corrupted tail
%% 4. delete the last one if it empty
%% repeat from 3 until no corruption found
%% Return log-id list with items sorted in reversed order
list_sort_truncate_files(Dir) ->
  Lids = list_sort_files(Dir),
  maybe_truncate_files(Dir, Lids).

%% list all segment files and return them in reversed order
list_sort_files(Dir) ->
  Match = filename:join([Dir, "*."?SUFFIX]),
  All = filelib:wildcard(Match),
  lists:reverse(
    lists:sort(
      lists:map(fun(F) -> parse_lid_from_filename(F) end, All))).

maybe_truncate_files(_Dir, []) -> [];
maybe_truncate_files(Dir, [?LID(Gnr, Idx) | Rest] = All) ->
  File = filename(Dir, Gnr, Idx),
  ok = maybe_truncate(File, Idx),
  case filelib:file_size(File) > 0 of
    true ->
      All;
    false ->
      ?log_info("Deleting empty segment file ~s\n", [File]),
      ok = file:delete(File),
      maybe_truncate_files(Dir, Rest)
  end.

maybe_truncate(File, Idx) ->
  Fd = open_file(File),
  try
    case scan(Fd, Idx) of
      ok ->
        ok;
      {truncate, Pos, Reason} ->
        ?log_warn("Truncating rlog ~s at position ~p, due to:\n~p",
                  [File, Pos, Reason]),
        {ok, _} = file:position(Fd, Pos),
        ok = file:truncate(Fd)
    end
  after
    ok = file:close(Fd)
  end.

%% Scan the whole file to detect corruption.
%% return 'ok' if there is nothing corrupted.
%% return '{truncate, Pos, Reason}' in case the file is corrupted
scan(Fd, Idx) ->
  {ok, Pos} = file:position(Fd, {cur, 0}),
  case file:read(Fd, ?ENTRY_HEADER_BYTES) of
    eof ->
      ok;
    {ok, ?V0_HEADER(CRC, Size)} ->
      try scan_entry(Fd, CRC, Size, Idx) of
        ok ->
          scan(Fd, Idx + 1)
      catch
        throw : Reason ->
          {truncate, Pos, Reason}
      end;
    {ok, Other} ->
      {truncate, Pos, {bad_entry_header, Other}}
  end.

scan_entry(Fd, CRC, Size, ExpectedIdx) ->
  case file:read(Fd, ?V0_BODY_BYTES(Size)) of
    {ok, ?V0_BODY(Size, Entry, Idx)} ->
      CRC =:= erlang:crc32(Entry) orelse throw(bad_crc),
      Idx =:= ExpectedIdx orelse throw({bad_index, Idx, ExpectedIdx}),
      ok;
    {ok, Other} ->
      erlang:throw({bad_entry_body, Other});
    eof ->
      erlang:throw({bad_entry_body, eof})
  end.

apply_defaults(Cfg) ->
  Defaults = #{?rlog_seg_bytes => ?DEFAULT_SEG_BYTES},
  maps:merge(Defaults, Cfg).

is_new_segment(#{last_lid := ?LID(Last, _)}, Gnr) when Gnr > Last ->
  true;
is_new_segment(#{ cfg := #{?rlog_seg_bytes := SegBytes}
                , fd_bytes := FdBytes}, _Gnr) when FdBytes >= SegBytes ->
  true;
is_new_segment(_, _) ->
  false.
