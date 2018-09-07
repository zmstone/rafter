%% @doc Raft log queue.

-module(raft_lq).

-export([new/1, in/3, out/1, to_list/1, get_last_lid/1]).
-export([count/1, is_empty/1]).

-export_type([lq/0]).

-include("raft_int.hrl").

-define(ITEM(Lid, Entry), {Lid, Entry}).

-type lid() :: raft:lid().
-type epoch() :: raft:epoch().
-type index() :: raft:index().  % non_neg_integer
-type count() :: non_neg_integer().
-type entry() :: term().
-type item() :: {epoch(), entry()}.

-opaque lq() :: #{ f_index := index() % index for first item
                 , n_index := index() % index for next item
                 , queue   := queue:queue(item())
                 }.

-spec new(index()) -> lq().
new(Index) ->
  #{ f_index => Index
   , n_index => Index
   , queue   => queue:new()
   }.

-spec in(lq(), epoch(), entry()) -> lq().
in(#{ n_index := N_Index
    , queue   := Queue
    } = Q, Epoch, Entry) ->
  Q#{ n_index := N_Index + 1
    , queue   := queue:in(?ITEM(Epoch, Entry), Queue)
    }.

-spec out(lq()) -> empty | {{lid(), entry()}, lq()}.
out(#{ f_index := F_Index
     , queue   := Queue
     } = Q) ->
  case is_empty(Q) of
    true ->
      empty;
    false ->
      {{value, ?ITEM(Epoch, Entry)}, NewQueue}= queue:out(Queue),
      Result = {?LID(Epoch, F_Index), Entry},
      NewQ = Q#{ f_index := F_Index + 1
               , queue   := NewQueue
               },
      {Result, NewQ}
  end.

-spec count(lq()) -> count().
count(#{f_index := F, n_index := N}) -> N - F.

-spec is_empty(lq()) -> boolean().
is_empty(#{queue := Queue} = Q) ->
  case count(Q) of
    0 -> true = queue:is_empty(Queue); % assert
    _ -> false
  end.

-spec to_list(lq()) -> [{lid(), entry()}].
to_list(#{f_index := F_Index,
          n_index := N_Index,
          queue := Queue}) ->
  to_list(F_Index, N_Index, Queue, []).

-spec get_last_lid(lq()) -> empty | lid().
get_last_lid(#{n_index := N_Index, queue := Q}) ->
  case queue:peek_r(Q) of
    empty -> empty;
    {value, ?ITEM(Epoch, _Entry)} -> ?LID(Epoch, N_Index - 1)
  end.

%%%*_/ internal functions ======================================================

to_list(Max, Max, Queue, Acc) ->
  true = queue:is_empty(Queue), % assert
  lists:reverse(Acc);
to_list(Index, Max, Queue, Acc) ->
  {{value, {Epoch, Entry}}, Rest} = queue:out(Queue),
  to_list(Index + 1, Max, Rest, [{?LID(Epoch, Index), Entry} | Acc]).

