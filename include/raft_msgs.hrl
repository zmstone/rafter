-ifndef(RAFT_MSGS_HRL).
-define(RAFT_MSGS_HRL, true).

-define(peer_down(PeerId), {peer_down, PeerId}).
-define(peer_connected(PeerId, SendFun), {peer_connected, PeerId, SendFun}).
-define(stop_connector, stop_connector).

%% We do not have heartbeats, use a step_down message to
%% notify peers (followers) to start a new election.
-define(step_down(LeaderId, Gnr), {step_down, LeaderId, Gnr}).

-define(vote_req(Id, Gnr, LastLid), {vote_req, Id, Gnr, LastLid}).
-define(vote_rsp(Id, Gnr, IsGranted), {vote_rsp, Id, Gnr, IsGranted}).

%% rlog: replicated log
-define(rlog_req(Id, Gnr, Args), {rlog_req, Id, Gnr, Args}).
-define(rlog_rsp(Id, Gnr, Result), {rlog_rsp, Id, Gnr, Result}).

-endif.

