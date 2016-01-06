-module(raft_leader).

-export([ become/2
        , handle_msg/2
        ]).

-export_type([ leader/0
             ]).

-include("gen_raft_private.hrl").

-define(leader, ?MODULE).

-record(?leader,
        {
        }).

-opaque leader() :: #?leader{}.

become(_InitArgs, #?state{cb_mod = CbMod, cb_state = CbState} = State) ->
  Leader = #?leader{},
  {ok, NewCbState} = CbMod:elected(CbState),
  gen_raft:loop(State#?state{ cb_state   = NewCbState
                            , raft_state = Leader
                            }).

handle_msg(_Msg, State) ->
  gen_raft:loop(State).

