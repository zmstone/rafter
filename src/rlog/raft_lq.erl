%% @doc Raft log queue.

-module(raft_lq).

-export([new/2, in/2, out/1, to_list/1, bump_epoch/1]).
-export([count/1, is_empty/1]).

-export_type([lq/0]).

-include("raft_int.hrl").

-type lid() :: raft:lid().
-type epoch() :: raft:epoch().
-type index() :: raft:index().  % non_neg_integer
-type count() :: non_neg_integer().
-type entry() :: raft_rlog:entry().
-type item() :: {epoch(), raft_rlog:entry()}.

-opaque lq() :: #{ epoch   := epoch() % current epoch
                 , f_index := index() % index for first item
                 , n_index := index() % index for next item
                 , queue   := queue:queue(item())
                 }.

-spec new(epoch(), index()) -> lq().
new(Epoch, Index) ->
  #{ f_index => Index
   , n_index => Index
   , epoch   => Epoch
   , queue   => queue:new()
   }.

-spec in(entry(), lq()) -> lq().
in(Entry, #{ n_index := N_Index
           , epoch   := Epoch
           , queue   := Queue
           } = Q) ->
  Q#{ n_index := N_Index + 1
    , queue   := queue:in(?LID(Epoch, Entry), Queue)
    }.

-spec out(lq()) -> empty | {{lid(), entry()}, lq()}.
out(#{ f_index := F_Index
     , queue   := Queue
     } = Q) ->
  case is_empty(Q) of
    true ->
      empty;
    false ->
      {{value, {Epoch, Entry}}, NewQueue}= queue:out(Queue),
      Result = {?LID(Epoch, F_Index), Entry},
      NewQ = Q#{ f_index := F_Index + 1
               , queue   := NewQueue
               },
      {Result, NewQ}
  end.

-spec bump_epoch(lq()) -> lq().
bump_epoch(#{epoch := Epoch} = Q) ->
  Q#{epoch => Epoch + 1}.

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

%%%*_/ internal functions ======================================================

to_list(Max, Max, Queue, Acc) ->
  true = queue:is_empty(Queue), % assert
  lists:reverse(Acc);
to_list(Index, Max, Queue, Acc) ->
  {{value, {Epoch, Entry}}, Rest} = queue:out(Queue),
  to_list(Index + 1, Max, Rest, [{?LID(Epoch, Index), Entry} | Acc]).

