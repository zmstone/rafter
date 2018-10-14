%% @doc This module manages an opaque structure
%% of replication log and the commit cursor.

-module(raft_rlog).

-export([open/2, close/1]).
-export([ cfg_keys/0
        , get_last_lid/1
        , get_last_committed_lid/1
        ]).
-export([ append/2
        , commit/2
        ]).

%% hidden
-export([do_commit/2]).

-export_type([ rlog/0
             , cfg_key/0
             , cfg/0
             , entry/0
             ]).

-include("raft_int.hrl").

-type lid() :: raft:lid().
-type my_cfg_key() :: atom(). %% TODO: be specific
-type my_cfg() :: #{my_cfg_key() => term()}.
-type cfg_key() :: my_cfg_key() | raft_rlog_segs:cfg_key().
-type cfg() :: #{cfg_key() => term()}.
-type dir() :: string().
-type gnr() :: raft:gnr().
-type idx() :: raft:idx().
-type entry() :: {idx(), raft_rlog_segs:entry()}.
-opaque rlog() :: #{ cfg := my_cfg()
                   , segs := raft_rlog_segs:segs()
                   , commit_lid :=  lid()
                   , committer := fun((lid()) -> ok)
                   }.

%% @doc Config keys for rlog.
-spec cfg_keys() -> [raft_rlog_segs:cfg_key() | my_cfg_key()].
cfg_keys() -> raft_rlog_segs:cfg_keys() ++ my_cfg_keys().

%% @doc Open.
-spec open(dir(), cfg()) -> rlog().
open(Dir, Cfg0) ->
  SegsCfg = maps:with(raft_rlog_segs:cfg_keys(), Cfg0),
  MyCfg = maps:with(my_cfg_keys(), Cfg0),
  Segs = raft_rlog_segs:open(Dir, SegsCfg),
  #{ cfg => MyCfg
   , segs => Segs
   , commit_lid => read_commit_lid(Dir)
   , committer => fun(Lid) -> ?MODULE:do_commit(Dir, Lid) end
   }.

%% @doc Close log file fd:s etc.
-spec close(rlog()) -> ok.
close(#{segs := Segs}) -> raft_rlog_segs:close(Segs).

%% @doc Return the id of last log entry.
-spec get_last_lid(rlog()) -> lid().
get_last_lid(#{segs := Segs}) ->
  raft_rlog_segs:get_last_lid(Segs).

%% @doc Return the id of last committed log entry.
-spec get_last_committed_lid(rlog()) -> lid().
get_last_committed_lid(#{commit_lid := Lid}) ->
  Lid.

%% @doc Append log entries.
-spec append(rlog(), [{gnr(), [entry()]}]) -> rlog().
append(Rlog, []) -> Rlog;
append(Rlog, [{Gnr, Entries} | Rest]) ->
  NewRlog = raft_rlog_segs:append(Rlog, Gnr, Entries),
  append(NewRlog, Rest).

%% @doc Commit log
-spec commit(rlog(), lid()) -> rlog().
commit(#{committer := F} = Rlog, Lid) ->
  ok = F(Lid),
  Rlog#{commit_lid := Lid}.

%%%*_/ internal functions ======================================================

%% @hidden
do_commit(Dir, Lid) ->
  File = commit_filename(Dir),
  IoData = io_lib:format("~p.\n", [Lid]),
  TmpFile = File ++ ".tmp",
  ok = file:write_file(TmpFile, IoData),
  ok = file:rename(TmpFile, File).

read_commit_lid(Dir) ->
  File = commit_filename(Dir),
  case file:consult(File) of
    {ok, [Lid]} -> Lid;
    {error, enoent} -> ?NO_PREV_LID
  end.

commit_filename(Dir) -> filename:join(Dir, "COMMIT").

%% TODO
my_cfg_keys() -> [].

