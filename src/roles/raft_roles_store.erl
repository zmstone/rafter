%% Module for persisted raft state.
%% epoch number as file name, file content includes:
%% voted_for: The Id of cluster member which got my vote in this epoch
%% changing_member: member being added or deleted
%% stable_members: all stable mebers in the cluster

-module(raft_roles_store).

-export([read/2, write/2, delete/2, ensure_deleted/2]).

-export_type([state/0]).

-define(STATE_FILE_SUFFIX, "raft").

-type dir() :: string().
-type epoch() :: raft:epoch().
-type member_id() :: raft:member_id().
-type changing_member() :: raft_role:changing_member().
-type state() :: #{ voted_for := none | member_id()
                  , stable_members := [member_id()]
                  , changing_member := changing_member()
                  }.

%%%*_/ APIs ====================================================================

-spec read(dir(), epoch()) -> state().
read(Dir, Epoch) ->
  {ok, Proplist} = file:consult(filename(Dir, Epoch)),
  maps:from_list(Proplist).

-spec write(dir(), raft_roles:data()) -> ok.
write(Dir, Data) ->
  #{ current_epoch := Epoch
   , voted_for := VotedFor
   , stable_members := Members
   , changing_member := ChangingMember
   } = Data,
  Filename = filename(Dir, Epoch),
  Proplist =
    [ {voted_for, VotedFor}
    , {stable_members, Members}
    , {changing_member, ChangingMember}
    ],
  IoData = lists:map(fun(I) -> io_lib:format("~p.\n", [I]) end, Proplist),
  TmpFile= tmp_filename(Filename),
  ok = file:write_file(TmpFile, IoData),
  ok = file:rename(TmpFile, Filename).

delete(Dir, Epoch) ->
  Filename = filename(Dir, Epoch),
  TmpFile = tmp_filename(Filename),
  ok = file:delete(Filename),
  ok = file:delete(TmpFile).

-spec ensure_deleted(dir(), all | {except, epoch()}) -> ok.
ensure_deleted(Dir, What) ->
  AllFiles = filelib:wildcard("*."?STATE_FILE_SUFFIX, Dir),
  AllEpoch = lists:map(fun parse_filename/1, AllFiles),
  lists:foreach(fun(Epoch) -> delete(Dir, Epoch) end,
                resolve_epoches(AllEpoch, What)).

%%%*_/ Internals ===============================================================

tmp_filename(Filename) -> Filename ++ ".tmp".

filename(Dir, Epoch) ->
  EpochStr = lists:flatten(io_lib:format("~10.10.0w", [Epoch])),
  filename:join([Dir, EpochStr]) ++ "."?STATE_FILE_SUFFIX.

parse_filename(Path) ->
  BaseName = filename:basename(Path, "."?STATE_FILE_SUFFIX),
  list_to_integer(BaseName).

resolve_epoches(AllEpoch, all) -> AllEpoch;
resolve_epoches(AllEpoch, {except, Epoch}) -> AllEpoch -- [Epoch].

