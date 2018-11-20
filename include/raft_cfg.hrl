-ifndef(RAFT_CFG_HRL).
-define(RAFT_CFG_HRL, true).

%% config entry names
-define(data_dir, data_dir).
-define(rlog_seg_bytes, rlog_seg_bytes).
-define(my_id, my_id).
-define(initial_members, initial_members).
-define(peer_conn_module, peer_conn_module).
-define(peer_conn_opts, peer_conn_opts).
-define(election_timeout_base, election_timeout_base).
-define(election_timeout_rand, election_timeout_rand).
-define(stay_down_timeout, stay_down_timeout).
-define(stm_implementation, stm_implementation).

%% default values
-define(ELECTION_TIMEOUT_BASE, 500).
-define(ELECTION_TIMEOUT_RAND, 500).
-define(STAY_DOWN_TIMEOUT, 10000).
-define(STM_IMPLEMENTATION, {raft_stm_simplekv,
                             #{dir => code:priv_dir(rafter)}}).

-endif.
