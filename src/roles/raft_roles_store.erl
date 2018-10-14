%% Module for persisted raft state.
%% Generation number as file name, file content includes:
%% voted_for: The Id of cluster member which got my vote in this generation
%% changing_member: member being added or deleted
%% stable_members: all stable mebers in the cluster

-module(raft_roles_store).

-export([read/2, write/2, delete/2, ensure_deleted/2]).

-export_type([state/0]).

-define(STATE_FILE_SUFFIX, "raft").

-type dir() :: string().
-type gnr() :: raft:gnr().
-type member_id() :: raft:member_id().
-type changing_member() :: raft_role:changing_member().
-type state() :: #{ voted_for := none | member_id()
                  , stable_members := [member_id()]
                  , changing_member := changing_member()
                  }.

%%%*_/ APIs ====================================================================

-spec read(dir(), gnr()) -> not_found | state().
read(Dir, Gnr) ->
  case file:consult(filename(Dir, Gnr)) of
    {ok, Proplist} -> maps:from_list(Proplist);
    {error, enoent} -> not_found
  end.

-spec write(dir(), raft_roles:data()) -> ok.
write(Dir, Data) ->
  #{ current_gnr:= Gnr
   , voted_for := VotedFor
   , stable_members := Members
   , changing_member := ChangingMember
   } = Data,
  Filename = filename(Dir, Gnr),
  Proplist =
    [ {voted_for, VotedFor}
    , {stable_members, Members}
    , {changing_member, ChangingMember}
    ],
  IoData = lists:map(fun(I) -> io_lib:format("~p.\n", [I]) end, Proplist),
  TmpFile= tmp_filename(Filename),
  ok = file:write_file(TmpFile, IoData),
  ok = file:rename(TmpFile, Filename).

delete(Dir, Gnr) ->
  Filename = filename(Dir, Gnr),
  TmpFile = tmp_filename(Filename),
  ok = file:delete(Filename),
  ok = file:delete(TmpFile).

-spec ensure_deleted(dir(), all | {except, gnr()}) -> ok.
ensure_deleted(Dir, What) ->
  AllFiles = filelib:wildcard("*."?STATE_FILE_SUFFIX, Dir),
  AllGnr = lists:map(fun parse_filename/1, AllFiles),
  lists:foreach(fun(Gnr) -> delete(Dir, Gnr) end,
                resolve_generation_nr(AllGnr, What)).

%%%*_/ Internals ===============================================================

tmp_filename(Filename) -> Filename ++ ".tmp".

filename(Dir, Gnr) ->
  GnrStr = lists:flatten(io_lib:format("~10.10.0w", [Gnr])),
  filename:join([Dir, GnrStr]) ++ "."?STATE_FILE_SUFFIX.

parse_filename(Path) ->
  BaseName = filename:basename(Path, "."?STATE_FILE_SUFFIX),
  list_to_integer(BaseName).

resolve_generation_nr(AllGnr, all) -> AllGnr;
resolve_generation_nr(AllGnr, {except, Gnr}) -> AllGnr -- [Gnr].

