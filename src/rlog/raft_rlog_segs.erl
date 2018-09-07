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
-type index() :: raft:index().
-type epoch() :: raft:epoch().
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
-define(INDEX_BYTES, 8).
-define(ENTRY_HEADER_BYTES, 9).
-define(INDEX_BIN(INDEX), <<INDEX:64/unsigned-integer>>).
-define(V0_HEADER(CRC, SIZE),
        <<?LAYOUT_VSN:8/unsigned-integer,
          CRC:32/unsigned-integer,
          SIZE:32/unsigned-integer>>).
-define(V0_BODY(SIZE, ENTRY, INDEX),
        <<ENTRY:SIZE/binary, INDEX:64/unsigned-integer>>).
-define(V0_LOG(CRC, SIZE, ENTRY, INDEX),
        <<?LAYOUT_VSN:8/unsigned-integer,
          CRC:32/unsigned-integer,
          SIZE:32/unsigned-integer,
          ENTRY:SIZE/binary,
          INDEX:64/unsigned-integer>>).
-define(V0_LOG_TAIL(CRC, SIZE, ENTRY, INDEX, REST),
        <<?LAYOUT_VSN:8/unsigned-integer,
          CRC:32/unsigned-integer,
          SIZE:32/unsigned-integer,
          ENTRY:SIZE/binary,
          INDEX:64/unsigned-integer,
          REST/binary>>).
-define(V0_BODY_BYTES(Size), (Size + ?INDEX_BYTES)).

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
    [?LID(Epoch, LastBaseIndex) | _] = Lids ->
      LastFile = filename(Dir, Epoch, LastBaseIndex),
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

-spec append(segs(), epoch(), [{index(), entry()}]) -> segs().
append(Segs, Epoch, Entries) ->
  #{ cfg       := #{dir := Dir}
   , last_lid  := ?LID(LastEpoch, LastIndex)
   , base_lids := BaseLids
   } = Segs,
  LastEpoch =< Epoch orelse
    erlang:error({non_monotonic_epoch, #{last => LastEpoch, got => Epoch}}),
  {Index, _} = hd(Entries),
  {NewLastLid, IoData} = encode_entries(LastIndex, Entries, []),
  case is_new_segment(Segs, Epoch) of
    true ->
      ok = close(Segs),
      NextFilename = filename(Dir, Epoch, Index),
      do_append(Segs#{ fd := open_file(NextFilename)
                     , fd_bytes := 0
                     , base_lids := [?LID(Epoch, Index) | BaseLids]
                     , last_lid := ?LID(Epoch, NewLastLid)
                     }, IoData);
    false ->
      do_append(Segs#{last_lid := ?LID(Epoch, NewLastLid)}, IoData)
  end.

%% @doc Decode entries from binary.
-spec decode_entries(binary()) -> [{index(), entry()}].
decode_entries(Bin) ->
  decode_entries(Bin, []).

%% @doc Read one entry, call @link decode_entries/2 to decode.
-spec read(segs(), index()) -> {epoch(), binary()}.
read(Segs, Index) ->
  read(Segs, Index, _ChunkSize = 0).

%% @doc Read a chunk of entries, call @link decode_entries/2 to decode.
%% The caller should ensure the index is in valid range,
%% raise `error(empty | too_old | not_seen)' exception otherwise.
-spec read(segs(), index(), bytes()) -> {epoch(), binary()}.
read(Segs, Index, ChunkSize) ->
  #{ base_lids := BaseLids
   , last_lid := LastLid
   , cfg := #{dir := Dir}
   , fd := Fd
   } = Segs,
  LastLid =:= false andalso error(empty),
  ?LID(LastEpoch, LastIndex) = LastLid,
  Index > LastIndex andalso error(not_seen),
  ?LID(Epoch, BaseIndex) = find_base_lid(BaseLids, Index),
  %% force a file sync before reading the current segment file
  case Epoch =:= LastEpoch of
    true -> file:sync(Fd);
    false -> ok
  end,
  File = filename(Dir, Epoch, BaseIndex),
  Chunk = seek_and_read(File, Index - BaseIndex, ChunkSize),
  {Epoch, Chunk}.

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
  BytesToRead = max(Size + ?INDEX_BYTES, ChunkSize - ?ENTRY_HEADER_BYTES),
  {ok, Bin} = file:read(Fd, BytesToRead),
  <<Header/binary, Bin/binary>>.

%% Seek reader fd to the start position of the n:th entry.
%% Use file:position to skip through the first n-1 entries.
seek(_Fd, 0) -> ok;
seek(Fd, N) ->
  {ok, ?V0_HEADER(_CRC, Size)} = file:read(Fd, ?ENTRY_HEADER_BYTES),
  {ok, _} = file:position(Fd, {cur, Size + ?INDEX_BYTES}),
  seek(Fd, N - 1).

find_base_lid([], _Index) -> error(too_old);
find_base_lid([?LID(Epoch, BaseIndex) | Rest], Index) ->
  case BaseIndex > Index of
    true -> find_base_lid(Rest, Index);
    false -> ?LID(Epoch, BaseIndex)
  end.

encode_entries(LastIndex, [], Acc) ->
  {LastIndex, lists:reverse(Acc)};
encode_entries(LastIndex, [{Index, Entry} | Rest], Acc0) ->
  LastIndex + 1 =/= Index andalso LastIndex =/= ?NO_PREV_INDEX andalso
    erlang:error({non_consecutive_index, #{last => LastIndex, got => Index}}),
  Acc = [encode_entry(Index, Entry) | Acc0],
  encode_entries(Index, Rest, Acc).

encode_entry(Index, Entry) ->
  CRC = erlang:crc32(Entry),
  Size = bytes(Entry),
  ?V0_LOG(CRC, Size, Entry, Index).

decode_entries(?V0_LOG_TAIL(CRC, Size, Entry, Index, Rest), Acc) ->
  CRC =:= erlang:crc32(Entry) orelse erlang:error(bad_crc),
  decode_entries(Rest, [{Index, Entry} | Acc]);
decode_entries(<<V:8, _/binary>>, _Acc) ->
  erlang:error({bad_layout_vsn, V});
decode_entries(_, Acc) ->
  lists:reverse(Acc).

%% open file to append log entries
-spec open_and_read_last_lid(filename()) -> {lid(), file:fd()}.
open_and_read_last_lid(Filename) ->
  ?LID(Epoch, BaseIndex) = parse_lid_from_filename(Filename),
  Fd = open_file(Filename),
  {ok, EofPos} = file:position(Fd, eof),
  {ok, _} = file:position(Fd, EofPos - ?INDEX_BYTES),
  {ok, <<Index:64/unsigned-integer>>} = file:read(Fd, ?INDEX_BYTES),
  true = (Index >= BaseIndex), % assert
  {?LID(Epoch, Index), Fd}.

open_file(Filename) ->
  {ok, Fd} = file:open(Filename, [raw, read, write, binary]),
  Fd.

%% parse log-id (epoch + index) from segment file name
-spec parse_lid_from_filename(filename()) -> lid().
parse_lid_from_filename(Filename) ->
  BaseName = filename:basename(Filename, "."?SUFFIX),
  [Epoch, Index] = string:tokens(BaseName, "."),
  ?LID(list_to_integer(Epoch), list_to_integer(Index)).

%% make segment file name from log-id (epoch + index)
-spec filename(dir(), epoch(), index()) -> filename().
filename(Dir, Epoch, Index) ->
  EpochStr = lists:flatten(io_lib:format("~10.10.0w", [Epoch])),
  IndexStr = lists:flatten(io_lib:format("~20.10.0w", [Index])),
  filename:join([Dir, EpochStr ++ "." ++ IndexStr ++ "."?SUFFIX]).

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
maybe_truncate_files(Dir, [?LID(Epoch, Index) | Rest] = All) ->
  File = filename(Dir, Epoch, Index),
  ok = maybe_truncate(File, Index),
  case filelib:file_size(File) > 0 of
    true ->
      All;
    false ->
      ?log_info("Deleting empty segment file ~s\n", [File]),
      ok = file:delete(File),
      maybe_truncate_files(Dir, Rest)
  end.

maybe_truncate(File, Index) ->
  Fd = open_file(File),
  try
    case scan(Fd, Index) of
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
scan(Fd, Index) ->
  {ok, Pos} = file:position(Fd, {cur, 0}),
  case file:read(Fd, ?ENTRY_HEADER_BYTES) of
    eof ->
      ok;
    {ok, ?V0_HEADER(CRC, Size)} ->
      try scan_entry(Fd, CRC, Size, Index) of
        ok ->
          scan(Fd, Index + 1)
      catch
        throw : Reason ->
          {truncate, Pos, Reason}
      end;
    {ok, Other} ->
      {truncate, Pos, {bad_entry_header, Other}}
  end.

scan_entry(Fd, CRC, Size, ExpectedIndex) ->
  case file:read(Fd, ?V0_BODY_BYTES(Size)) of
    {ok, ?V0_BODY(Size, Entry, Index)} ->
      CRC =:= erlang:crc32(Entry) orelse throw(bad_crc),
      Index =:= ExpectedIndex orelse throw({bad_index, Index, ExpectedIndex}),
      ok;
    {ok, Other} ->
      erlang:throw({bad_entry_body, Other});
    eof ->
      erlang:throw({bad_entry_body, eof})
  end.

apply_defaults(Cfg) ->
  Defaults = #{?rlog_seg_bytes => ?DEFAULT_SEG_BYTES},
  maps:merge(Defaults, Cfg).

is_new_segment(#{last_lid := ?NO_PREV_LID, fd := Fd}, _Epoch) ->
  false = Fd, % assert
  true;
is_new_segment(#{last_lid := ?LID(Last, _)}, Epoch) when Epoch > Last ->
  true;
is_new_segment(#{ cfg := #{?rlog_seg_bytes := SegBytes}
                , fd_bytes := FdBytes}, _Epoch) when FdBytes >= SegBytes ->
  true;
is_new_segment(_, _) ->
  false.
