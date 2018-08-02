-ifndef(RAFT_MSGS_HRL).
-define(RAFT_MSGS_HRL, true).

-define(peer_down(PeerId), {peer_down, PeerId}).
-define(peer_connected(PeerId, SendFun), {peer_connected, PeerId, SendFun}).
-define(stop_connector, stop_connector).
-define(vote_request(Id, Epoch, LastLid), {vote_request, Id, Epoch, LastLid}).
-define(vote_granted(Id, Epoch), {vote_granted, Epoch, Id}).
-define(leader_announcement(Id, Epoch), {leader_announcement, Id, Epoch}).

-endif.

