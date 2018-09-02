-ifndef(RAFT_MSGS_HRL).
-define(RAFT_MSGS_HRL, true).

-define(peer_down(PeerId), {peer_down, PeerId}).
-define(peer_connected(PeerId, SendFun), {peer_connected, PeerId, SendFun}).
-define(stop_connector, stop_connector).

%% We do not have heartbeats, use a step_down message to
%% notify peers (followers) to start a new election.
-define(step_down(LeaderId, Epoch), {step_down, LeaderId, Epoch}).

-define(vote_req(Id, Epoch, LastLid), {vote_req, Id, Epoch, LastLid}).
-define(vote_rsp(Id, Epoch, IsGranted), {vote_rsp, Id, Epoch, IsGranted}).

%% rlog: replicated log
-define(rlog_req(Id, Epoch, Args), {rlog_req, Id, Epoch, Args}).
-define(rlog_rsp(Id, Epoch, Result), {rlog_rsp, Id, Epoch, Result}).

-endif.

