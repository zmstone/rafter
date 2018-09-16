-ifndef(RAFT_INT_HRL).
-define(RAFT_INT_HRL, true).

%% {Epoch = 0, Index = 0} works as the very first log entry
%% but actually a 'phony' which nver gets created
%% Real log entry index starts from 1.
-define(NO_PREV_EPOCH, 0).
-define(NO_PREV_INDEX, 0).
-define(LID(Epoch, Index), {Epoch, Index}).
-define(NO_PREV_LID, ?LID(?NO_PREV_EPOCH, ?NO_PREV_INDEX)).

-define(log_debug(Fmt, Args), logger:debug(Fmt, Args)).
-define(log_info(Fmt, Args),  logger:info(Fmt, Args)).
-define(log_warn(Fmt, Args),  logger:warning(Fmt, Args)).
-define(log_error(Fmt, Args), logger:error(Fmt, Args)).

-define(IS_MAJORITY(Count, Total), (Count > (Total) div 2)).
-endif.
