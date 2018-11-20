-ifndef(RAFT_INT_HRL).
-define(RAFT_INT_HRL, true).

%% {Gnr = 0, Idx = 0} works as the very first log entry
%% but actually a 'phony' which nver gets created
%% Real log entry index starts from 1.
-define(NO_PREV_GNR, 0).
-define(NO_PREV_IDX, 0).
-define(LID(Gnr, Idx), {Gnr, Idx}).
-define(NO_PREV_LID, ?LID(?NO_PREV_GNR, ?NO_PREV_IDX)).

-define(log_debug(Fmt, Args), logger:debug(Fmt, Args)).
-define(log_info(Fmt, Args),  logger:info(Fmt, Args)).
-define(log_warn(Fmt, Args),  logger:warning(Fmt, Args)).
-define(log_error(Fmt, Args), logger:error(Fmt, Args)).

-define(IS_MAJORITY(Count, Total), (Count > (Total) div 2)).

-define(ASSERT(Expr, Error),
        case Expr of
          true -> ok;
          false -> erlang:error(Error)
        end).

-define(ASSERT_MONOTONIC_GNR(LastGnr, Gnr),
        ?ASSERT(LastGnr =< Gnr,
                {non_monotonic_generation_nr, #{last => LastGnr, got => Gnr}})).

-define(ASSERT_CONSECUTIVE_IDX(LastIdx, Idx),
        ?ASSERT(LastIdx + 1 =:= Idx,
                {non_consecutive_index, #{last => LastIdx, got => Idx}})).

-endif.
