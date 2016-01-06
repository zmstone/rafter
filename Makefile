PROJECT = gen_raft
PROJECT_DESCRIPTION = High level raft abstraction
PROJECT_VERSION = 0.0.1

include erlang.mk

ERLC_OPTS += -DAPPLICATION=gen_raft
TEST_ERLC_OPTS += -DAPPLICATION=gen_raft
