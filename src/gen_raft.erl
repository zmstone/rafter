-module(gen_raft).

%% public APIs
-export([ create_node/3
        , is_leader/1
        , start/5
        , start_link/5
        , stop/1
        ]).

%% internal exports
-export([ loop/1
        , init_it/6
        , put_raft_meta/2
        , terminate/2
        ]).

%% system calls
-export([ system_continue/3
        , system_terminate/4
        , system_code_change/4
        , format_status/2
        ]).

-include("gen_raft_private.hrl").

-type cb_state() :: term().
-type state() :: #?state{}.
-define(raft_state_name(State), element(1, State#?state.raft_state)).

%%%*_/ Callback definitions ====================================================

-callback init(CbModArgs :: term()) ->
            {ok, CbState :: cb_state()} | {error, Reason :: any()}.

-callback terminate(Reason :: term(), CbModArgs :: term()) -> any().

-callback elected(CbState :: cb_state()) -> {ok, NewCbState :: cb_state()}.

%%%*_/ APIs ====================================================================

%% @doc TODO add doc
-spec start(Name :: atom(), InitArgs :: raft_init_args(),
            CbMod :: module(), CbModArgs :: term() , Options :: list()) ->
                {ok, pid()} | {error, any()}.
start(Name, InitArgs, CbMod, CbModArgs, Options) when is_atom(Name) ->
  gen:start(?MODULE, nolink, {local, Name},
            CbMod, {InitArgs, CbModArgs}, Options).

%% @doc TODO
-spec start_link(Name :: atom(), InitArgs :: raft_init_args(),
                 CbMod :: module(), CbModArgs :: term() , Options :: list()) ->
                    {ok, pid()} | {error, any()}.
start_link(Name, InitArgs, CbMod, CbModArgs, Options) when is_atom(Name) ->
  gen:start(?MODULE, link, {local, Name},
            CbMod, {InitArgs, CbModArgs}, Options).

-spec stop(raft_name() | pid()) -> ok.
stop(?undef) -> ok;
stop(Name) when is_atom(Name) -> stop(whereis(Name));
stop(Pid) when is_pid(Pid) ->
  Mref = erlang:monitor(process, Pid),
  ok = cast_priv(Pid, stop),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

-spec create_node(filename:name_all(), raft_peer(), [raft_peer()]) -> ok.
create_node(MetadataDir, ?raft_peer(_, Name) = MyId, Peers) ->
  {ok, RaftMeta} = raft_meta:create(MyId, ordsets:from_list([MyId | Peers])),
  Filename = filename:join(MetadataDir, metadata_filename(Name)),
  case file:read_file_info(Filename) of
    {ok, _}         -> ?warn("overwritting ~s\n", [Filename]);
    {error, enoent} -> ok
  end,
  {ok, IoData} = raft_meta:serialize(RaftMeta),
  ok = file:write_file(Filename, IoData).

%% @doc Return true if the process is in leader state.
-spec is_leader(raft_name() | pid()) -> boolean().
is_leader(Name_or_Pid) ->
  call_priv(Name_or_Pid, is_leader).

%%%*_/ Private APIs ============================================================

%% @hidden
init_it(Starter, self, {local, Name}, CbMod, {InitArgs, CbModArgs}, Opts) ->
  init_it(Starter, self(), {local, Name}, CbMod, {InitArgs, CbModArgs}, Opts);
init_it(Starter, Parent, {local, Name}, CbMod, {InitArgs, CbModArgs}, Opts) ->
  _ = random:seed(os:timestamp()),
  case CbMod:init(CbModArgs) of
    {ok, CbState} ->
      State = #?state{ parent     = Parent
                     , name       = Name
                     , cb_mod     = CbMod
                     , cb_state   = CbState
                     , raft_state = ?undef
                     , debug      = proplists:get_value(debug, Opts, [])
                     },
      proc_lib:init_ack(Starter, {ok, self()}),
      self() ! ?gen_raft_init(InitArgs),
      try
        loop(State)
      catch error : E ->
        Stacktrace = erlang:get_stacktrace(),
        terminate({E, Stacktrace}, State)
      end;
    {error, Reason} ->
      exit(Reason)
  end.

%% @hidden The main loop.
loop(#?state{parent = Parent, debug = Debug} = State) ->
  receive
    {'EXIT', Parent, Reason} ->
      %% in case callback module is trapping exit
      terminate(Reason, State);
    {system, From, Req} ->
      _ = sys:handle_system_msg(Req, From, Parent, ?MODULE, Debug, State),
      ?MODULE:loop(State);
    Msg when Debug =:= [] ->
      handle_msg(Msg, State);
    Msg ->
      Debug1 = sys:handle_debug(Debug, fun print_msg/3, State, Msg),
      handle_msg(Msg, State#?state{debug = Debug1})
  end.

%% @hidden
system_continue(_Parent, _Debug, State) ->
  ?MODULE:loop(State).

%% @hidden
system_terminate(Reason, _Parent, _Debug, State) ->
  terminate(Reason, State).

%% @hidden
system_code_change(State, _Module, _OldVsn, _Extra) ->
  %% TODO call callback module's code_change API
  {ok, State}.

%% @hidden
format_status(Opt, Status) ->
  %% [PDict, SysState, Parent, Debug, State] = Status,
  %% TODO format status
  %% TODO call callback module's format_status API if exported
  {Opt, Status}.

%% @doc Put raft metadata to gen_raft looping state, call callback module.
%% to update it too. whether or not flush to disk it is up to the callback
%% implementation.
%% @end
-spec put_raft_meta(raft_meta(), #?state{}) ->
        {ok, #?state{}} | no_return().
put_raft_meta(NewMeta, #?state{raft_meta = OldMeta} = State0) ->
  case OldMeta =:= NewMeta of
    true ->
      {ok, State0};
    false ->
      State = State0#?state{raft_meta = NewMeta},
      do_put_raft_meta(State)
  end.

%%%*_/ internal functions ======================================================

handle_msg({'$gen_raft', _} = Msg, State) ->
  handle_gen_raft_msg(Msg, State);
handle_msg({'$raft', _} = Msg, State)
 when ?raft_state_name(State) =:= raft_follower ->
  raft_follower:handle_msg(Msg, State);
handle_msg({'$raft', _} = Msg, State)
 when ?raft_state_name(State) =:= raft_candidate ->
  raft_candidate:handle_msg(Msg, State);
handle_msg({'$raft', _} = Msg, State)
 when ?raft_state_name(State) =:= raft_leader ->
  raft_leader:handle_msg(Msg, State);
handle_msg(Msg, #?state{name = Name} = State) ->
  ?error("~p ~p: discarded unknown msg: ~p\n", [?MODULE, Name, Msg]),
  ?MODULE:loop(State).

-spec handle_gen_raft_msg(term(), #?state{}) -> no_return().
handle_gen_raft_msg(?gen_raft_init(InitArgs),
                    #?state{ name       = Name
                           , raft_meta  = RaftMeta0
                           , raft_state = RaftState0
                           } = State) ->
  RaftState0 = ?undef, %% assert
  RaftMeta0 = ?undef, %% assert
  {ok, MetadataFd} = open_metadata_fd(Name, InitArgs),
  case get_raft_meta(Name, MetadataFd) of
    {ok, RaftMeta} ->
      {ok, RaftState} = raft_follower:init(RaftMeta, InitArgs),
      gen_raft:loop(
        State#?state{ raft_meta  = RaftMeta
                    , raft_state = RaftState
                    , meta_fd    = MetadataFd
                    });
    {error, Reason} ->
      terminate(Reason, State)
  end;
handle_gen_raft_msg(?gen_raft_cast(Msg), State) ->
  NewState = handle_gen_raft_cast(Msg, State),
  ?MODULE:loop(NewState);
handle_gen_raft_msg(?gen_raft_call(Ref, From, Call), State) ->
  {Result, NewState} = handle_gen_raft_call(Call, State),
  Reply = ?gen_raft_reply(Ref, Result),
  ok = raft_utils:cast(From, Reply),
  ?MODULE:loop(NewState).

terminate(Reason, #?state{ name     = Name
                         , cb_mod   = CbMod
                         , cb_state = CbState
                         , debug    = Debug
                         }) ->
  try
    _ = CbMod:terminate(Reason, CbState)
  catch C : E ->
    ?error("gen_raft ~p bad terminate: ~p:~p\n~p",
           [CbMod, C, E, erlang:get_stacktrace()])
  end,
  case is_error_termination(Reason) of
    true  ->
      sys:print_log(Debug),
      ?error("gen_raft ~p terminated\nreason: ~p\n", [Name, Reason]);
    false ->
      ?info("gen_raft ~p terminated, reason: ~p\n", [Name, Reason])
  end,
  exit(Reason).

is_error_termination(normal)        -> false;
is_error_termination(shutdown)      -> false;
is_error_termination({shutdown, _}) -> false;
is_error_termination(_)             -> true.

-spec handle_gen_raft_call(Call :: term(), state()) ->
        {Result :: term(), state()}.
handle_gen_raft_call(is_leader, State) ->
  Result = (raft_leader =:= ?raft_state_name(State)),
  {Result, State}.

handle_gen_raft_cast(stop, State) ->
  terminate(normal, State).

print_msg(Device, Msg, State) ->
  do_print_msg(Device, "~p", [Msg], State).

do_print_msg(Device, Fmt, Args, #?state{name = Name}) ->
  io:format(Device, "[~s] ~p: " ++ Fmt ++ "~n", [ts(), Name | Args]).

ts() ->
  Now = os:timestamp(),
  {_, _, MicroSec} = Now,
  {{Y,M,D}, {HH,MM,SS}} = calendar:now_to_local_time(Now),
  lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0w ~.2.0w:~.2.0w:~.2.0w.~w",
                              [Y, M, D, HH, MM, SS, MicroSec])).

-spec do_put_raft_meta(state()) -> ok.
do_put_raft_meta(#?state{ raft_meta = RaftMeta
                        , meta_fd   = Fd
                        } = State) ->
  {ok, IoData} = raft_meta:serialize(RaftMeta),
  ok = file:pwrite(Fd, _Bof = 0, IoData),
  {ok, State}.

-spec open_metadata_fd(raft_name(), raft_init_args()) ->
        {ok, file:fd()} | no_return().
open_metadata_fd(Name, InitArgs) ->
  {Which, Dir} = get_metadata_dir(InitArgs),
  Filename = filename:join(Dir, metadata_filename(Name)),
  case file:read_file_info(Filename) of
    {ok, _FileInfo} ->
      ?info("using metadata file ~s from ~s\n", [Filename, Which]),
      do_open_metadata_fd(Filename);
    {error, Reason} ->
      erlang:error({Filename, Reason})
  end.

-spec do_open_metadata_fd(file:name_all()) -> {ok, file:fd()}.
do_open_metadata_fd(Filename) ->
  case file:open(Filename, [read, write, raw, binary]) of
    {ok, Fd} ->
      {ok, Fd};
    {error, Reason} ->
      erlang:error(Reason)
  end.

-spec metadata_filename(raft_name()) -> string().
metadata_filename(Name) ->
  atom_to_list(Name) ++ ".raftmeta".

-spec get_metadata_dir(raft_init_args()) -> {string(), file:name_all()}.
get_metadata_dir(InitArgs) ->
  case proplists:get_value(metadata_dir, InitArgs) of
    ?undef -> get_default_metadata_dir();
    Dir    -> {"InitArgs", Dir}
  end.

-spec get_default_metadata_dir() -> {string(), file:name_all()}.
get_default_metadata_dir() ->
  case application:get_env(?APPLICATION, metadata_dir) of
    {ok, Dir} ->
      {"application env", Dir};
    ?undef ->
      ?error("mandatory config metdata_dir not found\n", []),
      erlang:error(metdata_dir)
  end.

-spec get_raft_meta(raft_name(), file:fd()) -> {ok, raft_meta()}.
get_raft_meta(Name, Fd) ->
  {ok, _} = file:position(Fd, 0),
  {ok, Bin} = read_file(Fd, <<>>),
  raft_meta:deserialize(Name, Bin).

-spec read_file(file:fd(), binary()) -> binary().
read_file(Fd, Acc) ->
  case file:read(Fd, 1000000) of
    {ok, Data} -> read_file(Fd, <<Acc/binary, Data/binary>>);
    eof        -> {ok, Acc}
  end.

call_priv(?undef, _Call) ->
  erlang:error(noproc);
call_priv(Name, Call) when is_atom(Name) ->
  call_priv(whereis(Name), Call);
call_priv(Pid, Call) when is_pid(Pid) ->
  From = self(),
  Mref = erlang:monitor(process, Pid),
  ok = raft_utils:cast(Pid, ?gen_raft_call(Mref, From, Call)),
  receive
    {'DOWN', Mref, process, Pid, Reason} ->
      erlang:error(Reason);
    ?gen_raft_reply(Mref, Result) ->
      erlang:demonitor(Mref, [flush]),
      Result
  end.

cast_priv(Name_or_Pid, Msg) ->
  GenRaftMsg = ?gen_raft_cast(Msg),
  ok = raft_utils:cast(Name_or_Pid, GenRaftMsg).

