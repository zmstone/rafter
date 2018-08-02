-ifndef(RAFT_INT_HRL).
-define(RAFT_INT_HRL, true).

%% Log ID
-define(LID(Epoch, Index), {Epoch, Index}).

-define(log_debug(Fmt, Args), logger:debug(Fmt, Args)).
-define(log_info(Fmt, Args),  logger:info(Fmt, Args)).
-define(log_warn(Fmt, Args),  logger:warning(Fmt, Args)).
-define(log_error(Fmt, Args), logger:error(Fmt, Args)).

-endif.
