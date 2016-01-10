-module(raft_logs).

-export([ get_commitTick/1
        , get_lastTick/1
        , new/1
        , maybe_append/4
        ]).

-export_type([logs/0]).

-include("gen_raft_private.hrl").

-record(logs,
        { lastApplied  :: raft_tick()
        , commitTick   :: raft_tick()
        , lastTick     :: raft_tick()
        , entries = [] :: [raft_log()]
        }).

-opaque logs() :: #logs{}.

%%%*_/ APIs ====================================================================

-spec new(raft_tick()) -> {ok, logs()}.
new(?undef) ->
  new(?raft_tick(_Term = 0, _Index = 0));
new(LastApplied) ->
  Logs = #logs{ lastApplied = LastApplied
              , commitTick  = LastApplied
              , lastTick    = LastApplied
              },
  {ok, Logs}.

-spec get_lastTick(logs()) -> raft_tick().
get_lastTick(#logs{lastTick = LastTick}) -> LastTick.

-spec get_commitTick(logs()) -> raft_tick().
get_commitTick(#logs{commitTick = CommitTick}) -> CommitTick.

-spec maybe_append(logs(), [raft_log()], raft_tick(), raft_tick()) ->
        {ok, logs()} | false.
maybe_append(Logs, Entries, PrevTick, CommitTick) ->
  #logs{lastTick = LastTick} = Logs,
  case PrevTick > LastTick of
    true  -> false;
    false -> append(Logs, Entries, PrevTick, CommitTick)
  end.

%%%*_/ internal functions ======================================================

-spec append(logs(), [raft_log()], raft_tick(), raft_tick()) ->
        {ok, logs()} | false.
append(Logs0, Entries, PrevTick, LeaderCommitTick) ->
  #logs{commitTick = CommitTick} = Logs0,
  %% commited ticks should never be truncated, bug otherwise
  true = (CommitTick =< PrevTick),
  Logs = maybe_truncate(Logs0, PrevTick),
  do_append(Logs, Entries, PrevTick, LeaderCommitTick).

do_append(Logs, Entries, PrevTick, LeaderCommitTick) ->
  #logs{ lastTick = LastTick
       , entries  = Entries0
       } = Logs,
  LastTick = PrevTick, %% assert
  NewLogs =
    case Entries =:= [] of
      true ->
        Logs;
      false ->
        {NewLastTick, _} = lists:last(Entries),
        Logs#logs{ lastTick = NewLastTick
                 , entries  = Entries0 ++ Entries
                 }
    end,
  {ok, maybe_update_commit_tick(NewLogs, LeaderCommitTick)}.

maybe_update_commit_tick(#logs{ lastTick   = LastTick
                              , commitTick = CommitTick
                              } = Logs, LeaderCommitTick) ->
  NewCommitTick = min(max(CommitTick, LeaderCommitTick), LastTick),
  Logs#logs{commitTick = NewCommitTick}.

maybe_truncate(#logs{lastTick = Tk} = Logs, Tk) -> Logs;
maybe_truncate(#logs{entries = Entries} = Logs, PrevTick) ->
  NewEntries = truncate(lists:reverse(Entries), PrevTick),
  Logs#logs{entries = NewEntries}.

truncate([], _Tick) -> [];
truncate([{Tk, _} | _] = Entries, Tk) -> lists:reverse(Entries);
truncate([{Tk, _} | Rest], Tk_) when Tk > Tk_ -> truncate(Rest, Tk_).

%%%*_/ tests ===================================================================

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

truncate_test() ->
  ?assertEqual([], truncate([], 0)),
  ?assertEqual([], truncate([{3,0},{2,0},{1,0}], 0)),
  ?assertEqual([{1,0}], truncate([{2,0},{1,0}], 1)),
  ?assertEqual([{1,0},{2,0}], truncate([{2,0},{1,0}], 2)).

-endif.
